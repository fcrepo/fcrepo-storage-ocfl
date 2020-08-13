/*
 * Licensed to DuraSpace under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * DuraSpace licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fcrepo.storage.ocfl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.OcflObjectUpdater;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.FileChangeType;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Default OcflObjectSession implementation.
 *
 * @author pwinckles
 */
public class DefaultOcflObjectSession implements OcflObjectSession {

    private final String sessionId;
    private final MutableOcflRepository ocflRepo;
    private final String ocflObjectId;
    private final Path objectStaging;
    private final ObjectReader headerReader;
    private final ObjectWriter headerWriter;
    private final Runnable deregisterHook;

    private final DigestAlgorithm digestAlgorithm;
    private final OcflOption[] ocflOptions;
    private final VersionInfo versionInfo;
    private final Set<PathPair> deletePaths;
    private final Map<PathPair, String> digests;

    private CommitType commitType;
    private String rootResourceId;
    private boolean closed = false;
    private boolean deleteObject = false;

    public DefaultOcflObjectSession(final String sessionId,
                                    final MutableOcflRepository ocflRepo,
                                    final String ocflObjectId,
                                    final Path objectStaging,
                                    final ObjectMapper objectMapper,
                                    final CommitType commitType,
                                    final Runnable deregisterHook) {
        this.sessionId = Objects.requireNonNull(sessionId, "sessionId cannot be null");
        this.ocflRepo = Objects.requireNonNull(ocflRepo, "ocflRepo cannot be null");
        this.ocflObjectId = Objects.requireNonNull(ocflObjectId, "ocflObjectId cannot be null");
        this.objectStaging = Objects.requireNonNull(objectStaging, "objectStaging cannot be null");
        this.headerReader = objectMapper.readerFor(ResourceHeaders.class);
        this.headerWriter = objectMapper.writerFor(ResourceHeaders.class);
        this.commitType = Objects.requireNonNull(commitType, "commitType cannot be null");
        this.deregisterHook = Objects.requireNonNull(deregisterHook, "deregisterHook cannot be null");

        this.versionInfo = new VersionInfo();
        this.deletePaths = new HashSet<>();
        this.digests = new HashMap<>();
        this.ocflOptions = new OcflOption[] {OcflOption.MOVE_SOURCE, OcflOption.OVERWRITE};

        this.rootResourceId = loadRootResourceId();
        this.digestAlgorithm = identifyObjectDigestAlgorithm();
    }

    // TODO resource iteration

    @Override
    public String sessionId() {
        return sessionId;
    }

    @Override
    public String ocflObjectId() {
        return ocflObjectId;
    }

    @Override
    public synchronized void writeResource(final ResourceHeaders headers, final InputStream content) {
        enforceOpen();

        final var paths = resolvePersistencePaths(headers);

        final var contentPath = encode(paths.getContentFilePath());
        final var headerPath = encode(paths.getHeaderFilePath());

        deletePaths.remove(contentPath);
        deletePaths.remove(headerPath);

        if (content != null) {
            final var contentDst = createStagingPath(contentPath);
            var digest = getOcflDigest(headers.getDigests());

            if (digest == null) {
                final var messageDigest = digestAlgorithm.getMessageDigest();
                write(new DigestInputStream(content, messageDigest), contentDst);
                digest = Hex.encodeHexString(messageDigest.digest());
                addDigestHeader(digest, headers);
            } else {
                write(content, contentDst);
            }

            digests.put(contentPath, digest);

            headers.setContentPath(contentPath.path);
            headers.setContentSize(fileSize(contentDst));
        }

        final var headerDst = createStagingPath(headerPath);
        writeHeaders(headers, headerDst);
    }

    @Override
    public synchronized void deleteContentFile(final ResourceHeaders headers) {
        enforceOpen();

        final var resourceId = headers.getId();
        final var headerPath = encode(PersistencePaths.headerPath(rootResourceId(), resourceId));

        if (newInSession(headerPath)) {
            deleteResource(resourceId);
        } else {
            final var existingHeaders = readHeaders(resourceId);

            if (existingHeaders.getContentPath() != null) {
                final var path = encode(existingHeaders.getContentPath());
                deletePaths.add(path);
                digests.remove(path);
            }

            headers.setContentPath(null);

            final var headerDst = createStagingPath(headerPath);
            writeHeaders(headers, headerDst);
        }
    }

    @Override
    public synchronized void deleteResource(final String resourceId) {
        enforceOpen();

        if (Objects.equals(rootResourceId(), resourceId)) {
            deleteObject = true;
            deletePaths.clear();
            digests.clear();

            if (Files.exists(objectStaging)) {
                try {
                    FileUtils.deleteDirectory(objectStaging.toFile());
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to deleted staged files.", e);
                }
            }
        } else {
            final var headerPath = encode(PersistencePaths.headerPath(rootResourceId(), resourceId));
            final var existingHeaders = readHeaders(resourceId);

            deletePaths.add(headerPath);

            if (existingHeaders.getContentPath() != null) {
                final var path = encode(existingHeaders.getContentPath());
                deletePaths.add(path);
                digests.remove(path);
            }
        }
    }

    @Override
    public ResourceHeaders readHeaders(final String resourceId) {
        return readHeaders(resourceId, null);
    }

    @Override
    public ResourceHeaders readHeaders(final String resourceId, final String versionNumber) {
        final var headerPath = encode(PersistencePaths.headerPath(rootResourceId(), resourceId));
        final var headerStream = readStream(headerPath, resourceId, versionNumber);
        return readHeaders(headerStream);
    }

    @Override
    public ResourceContent readContent(final String resourceId) {
        return readContent(resourceId, null);
    }

    @Override
    public ResourceContent readContent(final String resourceId, final String versionNumber) {
        final var headers = readHeaders(resourceId, versionNumber);
        Optional<InputStream> contentStream = Optional.empty();
        if (headers.getContentPath() != null) {
            contentStream = Optional.of(readStream(encode(headers.getContentPath()), resourceId, versionNumber));
        }
        return new ResourceContent(contentStream, headers);
    }

    @Override
    public List<OcflVersionInfo> listVersions(final String resourceId) {
        final var headerPath = PersistencePaths.headerPath(rootResourceId(), resourceId);

        if (!fileExistsInOcfl(headerPath)) {
            final var encoded = encode(headerPath);
            if (Files.exists(stagingPath(encoded))) {
                return Collections.emptyList();
            } else {
                throw new NotFoundException(String.format("Resource %s was not found.", resourceId));
            }
        }

        return listFileVersions(resourceId, headerPath);
    }

    @Override
    public void versionCreationTimestamp(final OffsetDateTime timestamp) {
        versionInfo.setCreated(timestamp);
    }

    @Override
    public void versionAuthor(final String name, final String address) {
        versionInfo.setUser(name, address);
    }

    @Override
    public void versionMessage(final String message) {
        versionInfo.setMessage(message);
    }

    @Override
    public void commitType(final CommitType commitType) {
        this.commitType = Objects.requireNonNull(commitType, "commitType cannot be null");
    }

    @Override
    public synchronized void commit() {
        enforceOpen();
        closed = true;

        if (deleteObject) {
            ocflRepo.purgeObject(ocflObjectId);
        }

        if (!deletePaths.isEmpty() || Files.exists(objectStaging)) {
            deletePathsFromStaging();

            final var updater = createObjectUpdater();

            if (commitType == CommitType.UNVERSIONED
                    || (commitType == CommitType.NEW_VERSION && ocflRepo.hasStagedChanges(ocflObjectId))) {
                ocflRepo.stageChanges(ObjectVersionId.head(ocflObjectId), versionInfo, updater);

                if (commitType == CommitType.NEW_VERSION) {
                    ocflRepo.commitStagedChanges(ocflObjectId, versionInfo);
                }
            } else {
                ocflRepo.updateObject(ObjectVersionId.head(ocflObjectId), versionInfo, updater);
            }
        }

        cleanup();
    }

    @Override
    public synchronized void abort() {
        if (!closed) {
            closed = true;
            cleanup();
        }
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    private PersistencePaths resolvePersistencePaths(final ResourceHeaders headers) {
        final var resourceId = headers.getId();
        final PersistencePaths paths;

        if (InteractionModel.ACL.getUri().equals(headers.getInteractionModel())) {
            final var parentHeaders = readHeaders(headers.getParent());
            paths = PersistencePaths.aclResource(!InteractionModel.NON_RDF.getUri()
                            .equals(parentHeaders.getInteractionModel()),
                    resolveRootResourceId(resourceId), resourceId);
        } else if (InteractionModel.NON_RDF.getUri().equals(headers.getInteractionModel())) {
            paths = PersistencePaths.nonRdfResource(resolveRootResourceId(resourceId), resourceId);
        } else if (headers.getInteractionModel() != null) {
            paths = PersistencePaths.rdfResource(resolveRootResourceId(resourceId), resourceId);
        } else {
            throw new IllegalArgumentException(
                    String.format("Interaction model for resource %s must be populated.", resourceId));
        }

        return paths;
    }

    private InputStream readStream(final PathPair path, final String resourceId, final String versionNumber) {
        return readStreamOptional(path, versionNumber)
                .orElseThrow(() -> new NotFoundException(String.format("File %s was not found for resource %s",
                        path, resourceId)));
    }

    private Optional<InputStream> readStreamOptional(final PathPair path, final String versionNumber) {
        if (isOpen() && deletePaths.contains(path)) {
            return Optional.empty();
        }

        return readFromStaging(path).or(() -> readFromOcfl(path, versionNumber));
    }

    private Optional<InputStream> readFromStaging(final PathPair path) {
        final var stagingPath = stagingPath(path);

        if (Files.exists(stagingPath)) {
            try {
                return Optional.of(Files.newInputStream(stagingPath));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return Optional.empty();
    }

    private Optional<InputStream> readFromOcfl(final PathPair path, final String versionNumber) {
        try {
            if (!(deleteObject && isOpen())) {
                if (ocflRepo.containsObject(ocflObjectId)) {
                    final var object = ocflRepo.getObject(ObjectVersionId.version(ocflObjectId, versionNumber));
                    if (object.containsFile(path.path)) {
                        return Optional.of(object.getFile(path.path).getStream());
                    }
                }
            }
        } catch (edu.wisc.library.ocfl.api.exception.NotFoundException e) {
            return Optional.empty();
        }
        return Optional.empty();
    }

    private Path stagingPath(final PathPair path) {
        return objectStaging.resolve(path.encodedPath);
    }

    private Path createStagingPath(final PathPair path) {
        final var stagingPath = stagingPath(path);

        try {
            Files.createDirectories(stagingPath.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return stagingPath;
    }

    private void write(final InputStream content, final Path destination) {
        try {
            Files.copy(content, destination, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeHeaders(final ResourceHeaders headers, final Path destination) {
        try {
            headerWriter.writeValue(destination.toFile(), headers);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Consumer<OcflObjectUpdater> createObjectUpdater() {
        return updater -> {
            if (Files.exists(objectStaging)) {
                if (SystemUtils.IS_OS_WINDOWS) {
                    addDecodedPaths(updater, ocflOptions);
                } else {
                    updater.addPath(objectStaging, ocflOptions);
                }
            }

            digests.forEach((path, digest) -> {
                updater.addFileFixity(path.path, digestAlgorithm, digest);
            });

            deletePaths.forEach(path -> {
                updater.removeFile(path.path);
            });
        };
    }

    private void deletePathsFromStaging() {
        deletePaths.stream().map(this::stagingPath).forEach(path -> {
            if (Files.exists(path)) {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
    }

    private ResourceHeaders readHeaders(final InputStream stream) {
        try {
            return headerReader.readValue(stream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<OcflVersionInfo> listFileVersions(final String resourceId, final String headerPath) {
        final var headDesc = ocflRepo.describeVersion(ObjectVersionId.head(ocflObjectId));

        return ocflRepo.fileChangeHistory(ocflObjectId, headerPath).getFileChanges().stream()
                .filter(change -> change.getChangeType() == FileChangeType.UPDATE)
                // do not include changes that were made in the mutable head
                .filter(change -> !(headDesc.isMutable() && headDesc.getVersionId().equals(change.getVersionId())))
                .map(change -> {
                    return new OcflVersionInfo(resourceId, ocflObjectId,
                            change.getVersionId().toString(),
                            toMementoInstant(change.getTimestamp()));
                }).collect(Collectors.toList());
    }

    private boolean fileExistsInOcfl(final String path) {
        if (ocflRepo.containsObject(ocflObjectId)) {
            return ocflRepo.describeVersion(ObjectVersionId.head(ocflObjectId)).containsFile(path);
        }
        return false;
    }

    private boolean newInSession(final PathPair headerPath) {
        if (ocflRepo.containsObject(ocflObjectId)) {
            return !ocflRepo.describeVersion(ObjectVersionId.head(ocflObjectId)).containsFile(headerPath.path);
        }
        return true;
    }

    /**
     * Attempts to load the root resource id of the OCFL object. If the OCFL object does not exist, then null is
     * returned and the root resource id is populated on the first session write operation. If the object does
     * exist but it does not contain a root resource, then an exception is thrown.
     *
     * @return the root resource id, or null
     */
    private String loadRootResourceId() {
        if (ocflRepo.containsObject(ocflObjectId)) {
            final var stream = readFromOcfl(encode(PersistencePaths.ROOT_HEADER_PATH), null);

            if (stream.isPresent()) {
                final var headers = readHeaders(stream.get());
                return headers.getId();
            } else {
                throw new IllegalStateException(
                        String.format("OCFL object %s exists but it does not contain a root Fedora resource",
                                ocflObjectId));
            }
        }

        return null;
    }

    /**
     * This method should be called on write. It sets the root resource id to the specified resource id if the
     * root resource id has not already been set. Otherwise, the existing root resource id is returned. This
     * method SHOULD NOT be called from any other operation other than write.
     *
     * @param resourceId the write resource id
     * @return the resolved root resource id
     */
    private String resolveRootResourceId(final String resourceId) {
        if (rootResourceId == null) {
            rootResourceId = resourceId;
        }
        return rootResourceId;
    }

    /**
     * Returns the root resource id of the object. If the root resource id is null, then there are no resources
     * in the object and a NotFoundException is thrown. This method should be used when accessing the root resource
     * id from ALL methods EXCEPT write.
     *
     * @return the root resource id
     * @throws NotFoundException if there is no known root resource
     */
    private String rootResourceId() {
        if (rootResourceId != null) {
            return rootResourceId;
        }
        throw new NotFoundException("No resource found in object " + ocflObjectId);
    }

    private PathPair encode(final String value) {
        if (SystemUtils.IS_OS_WINDOWS) {
            final String encoded;
            if (value.contains("/")) {
                encoded = Arrays.stream(value.split("/"))
                        .map(s -> URLEncoder.encode(s, StandardCharsets.UTF_8))
                        .collect(Collectors.joining("/"));
            } else {
                encoded = URLEncoder.encode(value, StandardCharsets.UTF_8);
            }
            return new PathPair(value, encoded);
        }
        return new PathPair(value, value);
    }

    private void addDecodedPaths(final OcflObjectUpdater updater, final OcflOption... ocflOptions) {
        try (var paths = Files.walk(objectStaging)) {
            paths.filter(Files::isRegularFile).forEach(file -> {
                final var logicalPath = windowsStagingPathToLogicalPath(file);
                updater.addPath(file, logicalPath, ocflOptions);
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String windowsStagingPathToLogicalPath(final Path path) {
        final var normalized = objectStaging.relativize(path).toString()
                .replace("\\", "/");
        return URLDecoder.decode(normalized, StandardCharsets.UTF_8);
    }

    private void cleanup() {
        if (Files.exists(objectStaging)) {
            FileUtils.deleteQuietly(objectStaging.toFile());
        }
        deregisterHook.run();
    }

    private void enforceOpen() {
        if (closed) {
            throw new IllegalStateException(
                    String.format("Session %s is already closed!", sessionId));
        }
    }

    private Instant toMementoInstant(final OffsetDateTime timestamp) {
        return timestamp.toInstant().truncatedTo(ChronoUnit.SECONDS);
    }

    private long fileSize(final Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String getOcflDigest(final Collection<URI> headerDigests) {
        if (headerDigests != null) {
            for (var uri : headerDigests) {
                final var parts = uri.getSchemeSpecificPart().split(":");
                if (parts.length == 2 && digestAlgorithm.getJavaStandardName().equalsIgnoreCase(parts[0])) {
                    return parts[1];
                }
            }
        }
        return null;
    }

    private void addDigestHeader(final String digest, final ResourceHeaders headers) {
        if (headers.getDigests() == null) {
            headers.setDigests(new ArrayList<>());
        }
        headers.getDigests().add(digestUri(digest));
    }

    private URI digestUri(final String digest) {
        try {
            return new URI("urn", digestAlgorithm.getJavaStandardName() + ":" + digest, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private DigestAlgorithm identifyObjectDigestAlgorithm() {
        if (ocflRepo.containsObject(ocflObjectId)) {
            return ocflRepo.describeObject(ocflObjectId).getDigestAlgorithm();
        } else {
            return ocflRepo.config().getDefaultDigestAlgorithm();
        }
    }

    private static class PathPair {
        final String path;
        final String encodedPath;

        PathPair(final String path, final String encodedPath) {
            this.path = path;
            this.encodedPath = encodedPath;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final PathPair pathPair = (PathPair) o;
            return path.equals(pathPair.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path);
        }
    }

}
