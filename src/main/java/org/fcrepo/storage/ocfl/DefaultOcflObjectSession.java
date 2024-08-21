/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Streams;
import io.ocfl.api.MutableOcflRepository;
import io.ocfl.api.OcflObjectUpdater;
import io.ocfl.api.OcflOption;
import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.api.model.FileChangeType;
import io.ocfl.api.model.FileDetails;
import io.ocfl.api.model.ObjectVersionId;
import io.ocfl.api.model.OcflObjectVersion;
import io.ocfl.api.model.OcflObjectVersionFile;
import io.ocfl.api.model.VersionInfo;
import io.ocfl.api.model.VersionNum;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.fcrepo.storage.ocfl.cache.Cache;
import org.fcrepo.storage.ocfl.exception.InvalidContentException;
import org.fcrepo.storage.ocfl.exception.NotFoundException;
import org.fcrepo.storage.ocfl.exception.ValidationException;
import org.fcrepo.storage.ocfl.validation.HeadersValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Default OcflObjectSession implementation.
 *
 * @author pwinckles
 */
public class DefaultOcflObjectSession implements OcflObjectSession {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOcflObjectSession.class);

    private static final int SPLITERATOR_OPTS = Spliterator.NONNULL |
            Spliterator.DISTINCT |
            Spliterator.SIZED |
            Spliterator.SUBSIZED |
            Spliterator.IMMUTABLE;

    private final String sessionId;
    private final MutableOcflRepository ocflRepo;
    private final String ocflObjectId;
    private final Path objectStaging;
    private final ObjectReader headerReader;
    private final ObjectWriter headerWriter;
    private final Cache<String, ResourceHeaders> headersCache;
    private final Cache<String, String> rootIdCache;
    private final HeadersValidator headersValidator;
    private final boolean useUnsafeWrite;

    private final DigestAlgorithm digestAlgorithm;
    private final OcflOption[] ocflOptions;
    private final VersionInfo versionInfo;
    private final Set<PathPair> deletePaths;
    private final Map<PathPair, String> digests;
    private final Map<String, ResourceHeaders> stagedHeaders;
    private final Map<String, String> stagedHeaderPaths;

    private CommitType commitType;
    private String rootResourceId;
    private boolean isArchivalGroup;
    private boolean closed = false;
    private boolean rolledback = false;
    private boolean deleteObject = false;
    private boolean isNewObject = false;
    private VersionNum newVersionNum;
    private boolean hadMutableHeadBeforeCommit;

    public DefaultOcflObjectSession(final String sessionId,
                                    final MutableOcflRepository ocflRepo,
                                    final String ocflObjectId,
                                    final Path objectStaging,
                                    final ObjectReader headerReader,
                                    final ObjectWriter headerWriter,
                                    final CommitType commitType,
                                    final Cache<String, ResourceHeaders> headersCache,
                                    final Cache<String, String> rootIdCache,
                                    final HeadersValidator headersValidator,
                                    final boolean useUnsafeWrite) {
        this.sessionId = Objects.requireNonNull(sessionId, "sessionId cannot be null");
        this.ocflRepo = Objects.requireNonNull(ocflRepo, "ocflRepo cannot be null");
        this.ocflObjectId = Objects.requireNonNull(ocflObjectId, "ocflObjectId cannot be null");
        this.objectStaging = Objects.requireNonNull(objectStaging, "objectStaging cannot be null");
        this.headerReader = Objects.requireNonNull(headerReader, "headerReader cannot be null");
        this.headerWriter = Objects.requireNonNull(headerWriter, "headerWriter cannot be null");
        this.commitType = Objects.requireNonNull(commitType, "commitType cannot be null");
        this.headersCache = Objects.requireNonNull(headersCache, "headersCache cannot be null");
        this.rootIdCache = Objects.requireNonNull(rootIdCache, "rootIdCache cannot be null");
        this.headersValidator = Objects.requireNonNull(headersValidator, "headersValidator cannot be null");
        this.useUnsafeWrite = useUnsafeWrite;

        this.versionInfo = new VersionInfo();
        this.deletePaths = new HashSet<>();
        this.digests = new HashMap<>();
        this.stagedHeaders = new HashMap<>();
        this.stagedHeaderPaths = new HashMap<>();
        this.ocflOptions = new OcflOption[] {OcflOption.MOVE_SOURCE, OcflOption.OVERWRITE};

        loadRootResourceId();
        this.digestAlgorithm = identifyObjectDigestAlgorithm();
    }

    @Override
    public String sessionId() {
        return sessionId;
    }

    @Override
    public String ocflObjectId() {
        return ocflObjectId;
    }

    @Override
    public synchronized ResourceHeaders writeResource(final ResourceHeaders headers, final InputStream content) {
        enforceOpen();

        final var paths = resolvePersistencePaths(headers);

        final var contentPath = encode(paths.getContentFilePath());
        final var headerPath = encode(paths.getHeaderFilePath());

        Path contentDst = null;

        deletePaths.remove(contentPath);
        deletePaths.remove(headerPath);

        try {
            final var headersBuilder = ResourceHeaders.builder(headers);

            if (content != null) {
                contentDst = createStagingPath(contentPath);
                var digest = getOcflDigest(headers.getDigests());

                if (digest == null) {
                    // compute the digest that OCFL uses if it was not provided
                    final var messageDigest = digestAlgorithm.getMessageDigest();
                    write(new DigestInputStream(content, messageDigest), contentDst);
                    digest = Hex.encodeHexString(messageDigest.digest());
                    headersBuilder.addDigest(digestUri(digest));
                } else {
                    write(content, contentDst);
                }

                digests.put(contentPath, digest);

                final var fileSize = fileSize(contentDst);

                if (headers.getContentSize() != -1
                        && fileSize != headers.getContentSize()) {
                    throw new InvalidContentException(
                            String.format("Resource %s's file size does not match expectation." +
                                    " Expected: %s; Actual: %s",
                            headers.getId(), headers.getContentSize(), fileSize));
                }

                headersBuilder.withContentPath(contentPath.path)
                        .withContentSize(fileSize);
            }

            final var finalHeaders = headersBuilder.build();

            stageHeaders(finalHeaders, headerPath.path, paths);
            touchRelatedResources(finalHeaders);

            return finalHeaders;
        } catch (final RuntimeException e) {
            safeDelete(contentDst);
            throw e;
        }
    }

    @Override
    public synchronized void writeHeaders(final ResourceHeaders headers) {
        enforceOpen();

        final var paths = resolvePersistencePaths(headers);

        final var headerPath = encode(paths.getHeaderFilePath());

        deletePaths.remove(headerPath);

        stageHeaders(headers, headerPath.path, paths);
        touchRelatedResources(headers);
    }

    @Override
    public synchronized void deleteContentFile(final ResourceHeaders headers) {
        enforceOpen();
        ensureKnownRootResource();

        final var resourceId = headers.getId();
        final var paths = resolvePersistencePaths(headers);
        final var headerPath = encode(paths.getHeaderFilePath());

        if (newInSession(headerPath)) {
            deleteResource(resourceId);
        } else {
            final var existingHeaders = readHeaders(resourceId);

            if (existingHeaders.getContentPath() != null) {
                final var path = encode(existingHeaders.getContentPath());
                deletePaths.add(path);
                digests.remove(path);
            }

            final var finalHeaders = ResourceHeaders.builder(headers)
                    .withContentPath(null)
                    .withContentSize(-1)
                    .withDigests(null)
                    .build();

            stageHeaders(finalHeaders, headerPath.path, paths);
            touchRelatedResources(finalHeaders);
        }
    }

    @Override
    public synchronized void deleteResource(final String resourceId) {
        enforceOpen();
        ensureKnownRootResource();

        if (Objects.equals(rootResourceId(), resourceId)) {
            deleteObject = true;
            deletePaths.clear();
            digests.clear();
            stagedHeaders.clear();

            if (Files.exists(objectStaging)) {
                try {
                    FileUtils.deleteDirectory(objectStaging.toFile());
                } catch (final IOException e) {
                    throw new UncheckedIOException("Failed to deleted staged files.", e);
                }
            }
        } else {
            final var headerPath = encode(PersistencePaths.headerPath(rootResourceId(), resourceId));
            final var existingHeaders = readHeaders(resourceId);

            deletePaths.add(headerPath);
            stagedHeaders.remove(existingHeaders.getId());

            if (existingHeaders.getContentPath() != null) {
                final var path = encode(existingHeaders.getContentPath());
                deletePaths.add(path);
                digests.remove(path);
            }
        }
    }

    @Override
    public boolean containsResource(final String resourceId) {
        if (rootResourceId == null) {
            return false;
        }

        if (stagedHeaders.containsKey(resourceId)) {
            return true;
        }

        final var headerPath = encode(PersistencePaths.headerPath(rootResourceId(), resourceId));
        final var stream = readStreamOptional(headerPath, null);

        if (stream.isPresent()) {
            try {
                stream.get().close();
            } catch (final IOException e) {
                // Ignore
            }
            return true;
        }

        return false;
    }

    @Override
    public ResourceHeaders readHeaders(final String resourceId) {
        return readHeaders(resourceId, null);
    }

    @Override
    public ResourceHeaders readHeaders(final String resourceId, final String versionNumber) {
        ensureKnownRootResource();

        if (versionNumber == null && stagedHeaders.containsKey(resourceId)) {
            return stagedHeaders.get(resourceId);
        }

        final var headerPath = encode(PersistencePaths.headerPath(rootResourceId(), resourceId));

        if (isOpen() && deletePaths.contains(headerPath)) {
            throw notFoundException(headerPath, resourceId);
        }

        final var resolvedVersionNum = resolveVersionNumber(resourceId, versionNumber);

        return headersCache.get(cacheKey(resourceId, resolvedVersionNum), key -> {
            LOG.trace("Cache miss for {}", key);
            final var ocflFileDetails = getObjectVersionFile(headerPath, resolvedVersionNum)
                    .orElseThrow(() -> notFoundException(headerPath, resourceId));
            final var path = ocflFileDetails.getStorageRelativePath();
            final var headers = parseHeaders(ocflFileDetails.getStream());
            if (path != null) {
                headers.setStorageRelativePath(path);
            }
            return headers;
        });
    }

    @Override
    public ResourceContent readContent(final String resourceId) {
        return readContent(resourceId, null);
    }

    @Override
    public ResourceContent readContent(final String resourceId, final String versionNumber) {
        ensureKnownRootResource();

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
            if (stagedHeaders.containsKey(resourceId)) {
                return Collections.emptyList();
            } else {
                throw new NotFoundException(String.format("Resource %s was not found.", resourceId));
            }
        }

        return listFileVersions(resourceId, headerPath);
    }

    /**
     * This method is NOT currently using the ResourceHeader cache. It should not matter because this method is
     * currently only used when reindexing. If it is ever used anywhere else, we may want to figure out how to
     * get it to use the cache.
     *
     * @return ResourceHeader stream
     */
    @Override
    public Stream<ResourceHeaders> streamResourceHeaders() {
        final var headerPaths = listCommittedHeaders();

        deletePaths.forEach(path -> headerPaths.remove(path.path));
        headerPaths.removeAll(stagedHeaderPaths.values());

        final var it = headerPaths.iterator();

        return Streams.concat(stagedHeaders.values().stream(),
                StreamSupport.stream(Spliterators.spliterator(new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public ResourceHeaders next() {
                        final var next = it.next();
                        return parseHeaders(readStreamOptional(encode(next), null)
                                .orElseThrow(() ->
                                        new IllegalStateException("Unable to find resource header file " + next)));
                    }
                }, headerPaths.size(), SPLITERATOR_OPTS), false));
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
    public void invalidateCache(final String resourceId) {
        try {
            listVersions(resourceId).stream()
                .map(version -> cacheKey(resourceId, version.getVersionNumber()))
                .forEach(headersCache::invalidate);
        } catch (NotFoundException ignored) {
        }
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
            rootIdCache.invalidate(ocflObjectId);
            ocflRepo.purgeObject(ocflObjectId);
        }

        hadMutableHeadBeforeCommit = ocflRepo.hasStagedChanges(ocflObjectId);

        if (!deletePaths.isEmpty() || !stagedHeaders.isEmpty() || Files.exists(objectStaging)) {
            deletePathsFromStaging();

            final var updater = createObjectUpdater();

            if (commitType == CommitType.UNVERSIONED
                    || hasMutableHeadAndShouldCreateNewVersion(hadMutableHeadBeforeCommit)) {
                // Stage updates to mutable HEAD when auto-versioning disabled, or immediately before committing the
                // mutable HEAD to a version when auto-versioning is enabled.
                newVersionNum = ocflRepo.stageChanges(ObjectVersionId.head(ocflObjectId), versionInfo, updater)
                        .getVersionNum();
            } else {
                newVersionNum = ocflRepo.updateObject(ObjectVersionId.head(ocflObjectId), versionInfo, updater)
                        .getVersionNum();
            }
        }

        if (hasMutableHeadAndShouldCreateNewVersion(hadMutableHeadBeforeCommit)) {
            ocflRepo.commitStagedChanges(ocflObjectId, versionInfo);
        }

        if (newVersionNum != null) {
            moveStagedHeadersToCache(newVersionNum.toString());

            if (isNewObject) {
                rootIdCache.put(ocflObjectId, rootResourceId());
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
    public synchronized void rollback() {
        if (rolledback) {
            return;
        }

        rolledback = true;

        if (closed && newVersionNum != null) {
            if (hadMutableHeadBeforeCommit) {
                throw new IllegalStateException(String.format(
                        "Cannot rollback changes to object %s because manual versioning was used on this object.",
                        ocflObjectId));
            }

            LOG.info("Rolling back {} version {}", ocflObjectId, newVersionNum);

            if (isNewObject) {
                // Purge the object if it only has one version or if it is a newly created object with a mutable head
                rootIdCache.invalidate(ocflObjectId);
                ocflRepo.purgeObject(ocflObjectId);
            } else {
                ocflRepo.rollbackToVersion(ObjectVersionId.version(ocflObjectId, newVersionNum.previousVersionNum()));
            }
        } else {
            closed = true;
            cleanup();
        }
    }

    @Override
    public void close() {
        abort();
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
                    resolveRootResourceId(headers), resourceId);
        } else if (InteractionModel.NON_RDF.getUri().equals(headers.getInteractionModel())) {
            paths = PersistencePaths.nonRdfResource(resolveRootResourceId(headers), resourceId);
        } else if (headers.getInteractionModel() != null) {
            paths = PersistencePaths.rdfResource(resolveRootResourceId(headers), resourceId);
        } else {
            throw new IllegalArgumentException(
                    String.format("Interaction model for resource %s must be populated.", resourceId));
        }

        return paths;
    }

    private InputStream readStream(final PathPair path, final String resourceId, final String versionNumber) {
        return readStreamOptional(path, versionNumber)
                .orElseThrow(() -> notFoundException(path, resourceId));
    }

    private Optional<InputStream> readStreamOptional(final PathPair path, final String versionNumber) {
        if (isOpen() && deletePaths.contains(path)) {
            return Optional.empty();
        }

        if (versionNumber != null) {
            return readFromOcflOptional(path, versionNumber);
        }

        return readFromStaging(path).or(() -> readFromOcflOptional(path, versionNumber));
    }

    private Optional<InputStream> readFromStaging(final PathPair path) {
        final var stagingPath = stagingPath(path);

        if (Files.exists(stagingPath)) {
            try {
                return Optional.of(new BufferedInputStream(Files.newInputStream(stagingPath)));
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return Optional.empty();
    }

    private Optional<OcflObjectVersion> getObject(final String versionNumber) {
        try {
            if (!(deleteObject && isOpen())) {
                if (containsOcflObject()) {
                    return Optional.of(ocflRepo.getObject(ObjectVersionId.version(ocflObjectId, versionNumber)));
                }
            }
        } catch (final io.ocfl.api.exception.NotFoundException e) {
            return Optional.empty();
        }
        return Optional.empty();
    }

    private Optional<OcflObjectVersionFile> getObjectVersionFile(final PathPair path, final String versionNumber) {
        final var object = getObject(versionNumber);
        if (object.isPresent()) {
            try {
                if (object.get().containsFile(path.path)) {
                    return Optional.of(object.get().getFile(path.path));
                }
            } catch (final io.ocfl.api.exception.NotFoundException e) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private Optional<InputStream> readFromOcflOptional(final PathPair path, final String versionNumber) {
        final var object = getObject(versionNumber);
        if (object.isPresent()) {
            try {
                if (object.get().containsFile(path.path)) {
                    return Optional.of(object.get().getFile(path.path).getStream());
                }
            } catch (final io.ocfl.api.exception.NotFoundException e) {
                return Optional.empty();
            }
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
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }

        return stagingPath;
    }

    private void write(final InputStream content, final Path destination) {
        try {
            Files.copy(content, destination, StandardCopyOption.REPLACE_EXISTING);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void stageHeaders(final ResourceHeaders headers, final String headerPath, final PersistencePaths paths) {
        validateHeaders(headers, paths);
        stageHeadersNoValidation(headers, headerPath);
    }

    private void stageHeadersNoValidation(final ResourceHeaders headers, final String headerPath) {
        stagedHeaders.put(headers.getId(), headers);
        stagedHeaderPaths.put(headers.getId(), headerPath);
    }

    private void validateHeaders(final ResourceHeaders headers, final PersistencePaths paths) {
        final var rootResourceId = rootResourceId();

        try {
            if (Objects.equals(headers.getId(), rootResourceId)) {
                headersValidator.validate(paths, headers, headers);
            } else {
                headersValidator.validate(paths, headers, readHeaders(rootResourceId));
            }
        } catch (ValidationException e) {
            throw ValidationException.createForResource(headers.getId(), e.getProblems());
        }
    }

    private void touchRelatedResources(final ResourceHeaders headers) {
        // Touch the AG for non-ACL AG part updates
        if (isArchivalGroup
                && !Objects.equals(rootResourceId(), headers.getId())
                && !InteractionModel.ACL.getUri().equals(headers.getInteractionModel())) {
            LOG.debug("Touching AG {} after updating {}", rootResourceId(), headers.getId());
            touchResource(rootResourceId(), headers.getMementoCreatedDate());
        }

        if (InteractionModel.NON_RDF_DESCRIPTION.getUri().equals(headers.getInteractionModel())) {
            LOG.debug("Touching binary {} after updating {}", headers.getParent(), headers.getId());
            touchResource(headers.getParent(), headers.getMementoCreatedDate());
        } else if (InteractionModel.NON_RDF.getUri().equals(headers.getInteractionModel())) {
            final var descriptionId = headers.getId() + "/" + PersistencePaths.FCR_METADATA;
            LOG.debug("Touching binary description {} after updating {}", descriptionId, headers.getId());
            try {
                touchResource(descriptionId, headers.getMementoCreatedDate());
            } catch (final NotFoundException e) {
                // Ignore this exception because it just means that the binary description hasn't been created yet
            }
        }
    }

    private void touchResource(final String resourceId, final Instant timestamp) {
        final var headers = ResourceHeaders.builder(readHeaders(resourceId))
                .withMementoCreatedDate(timestamp)
                .build();

        final var headerPath = PersistencePaths.headerPath(rootResourceId(), resourceId);

        stageHeadersNoValidation(headers, headerPath);
    }

    private Consumer<OcflObjectUpdater> createObjectUpdater() {
        return updater -> {
            stagedHeaders.forEach((id, headers) -> {
                final var path = stagedHeaderPaths.get(id);
                try {
                    updater.writeFile(new ByteArrayInputStream(headerWriter.writeValueAsBytes(headers)),
                            path, ocflOptions);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Failed to serialize resource headers", e);
                }
            });

            if (Files.exists(objectStaging)) {
                try (final var paths = Files.walk(objectStaging)) {
                    paths.filter(Files::isRegularFile).forEach(file -> {
                        final var logicalPath = stagingPathToLogicalPath(file);
                        final var digest = digests.get(new PathPair(logicalPath, null));

                        if (useUnsafeWrite && digest != null) {
                            updater.unsafeAddPath(digest, file, logicalPath, ocflOptions);
                        } else {
                            updater.addPath(file, logicalPath, ocflOptions);
                            if (digest != null) {
                                updater.addFileFixity(logicalPath, digestAlgorithm, digest);
                            }
                        }
                    });
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            deletePaths.forEach(path -> {
                updater.removeFile(path.path);
            });
        };
    }

    private void deletePathsFromStaging() {
        deletePaths.stream().map(this::stagingPath).forEach(path -> {
            try {
                Files.deleteIfExists(path);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private ResourceHeaders parseHeaders(final InputStream stream) {
        try {
            return headerReader.readValue(stream);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<OcflVersionInfo> listFileVersions(final String resourceId, final String headerPath) {
        final var headDesc = ocflRepo.describeVersion(ObjectVersionId.head(ocflObjectId));

        return ocflRepo.fileChangeHistory(ocflObjectId, headerPath).getFileChanges().stream()
                .filter(change -> change.getChangeType() == FileChangeType.UPDATE)
                // do not include changes that were made in the mutable head
                .filter(change -> !(headDesc.isMutable() && headDesc.getVersionNum().equals(change.getVersionNum())))
                .map(change -> {
                    return new OcflVersionInfo(resourceId, ocflObjectId,
                            change.getVersionNum().toString(),
                            toMementoInstant(change.getTimestamp()));
                }).collect(Collectors.toList());
    }

    private boolean fileExistsInOcfl(final String path) {
        if (containsOcflObject()) {
            return ocflRepo.describeVersion(ObjectVersionId.head(ocflObjectId)).containsFile(path);
        }
        return false;
    }

    private boolean newInSession(final PathPair headerPath) {
        if (containsOcflObject()) {
            return !ocflRepo.describeVersion(ObjectVersionId.head(ocflObjectId)).containsFile(headerPath.path);
        }
        return true;
    }

    /**
     * Attempts to load the root resource id of the OCFL object. If the OCFL object does not exist, then null is
     * returned and the root resource id is populated on the first session write operation. If the object does
     * exist but it does not contain a root resource, then an exception is thrown.
     */
    private void loadRootResourceId() {
        if (containsOcflObject()) {
            rootResourceId = rootIdCache.get(ocflObjectId, ocflObjectId -> {
                final var object = getObjectVersionFile(encode(PersistencePaths.ROOT_HEADER_PATH), null);

                if (object.isPresent()) {
                    final var headers = parseHeaders(object.get().getStream());
                    final var path = object.get().getStorageRelativePath();
                    if (path != null) {
                        headers.setStorageRelativePath(path);
                    }
                    final var headVersion = ocflRepo.describeVersion(ObjectVersionId.head(ocflObjectId));
                    addToCache(headers.getId(), headVersion.getVersionNum().toString(), headers);
                    return headers.getId();
                } else {
                    throw new IllegalStateException(
                            String.format("OCFL object %s exists but it does not contain a root Fedora resource",
                                    ocflObjectId));
                }
            });

            final var headers = readHeaders(rootResourceId);
            isArchivalGroup = headers.isArchivalGroup();
        }
    }

    /**
     * This method should be called on write. It sets the root resource id to the specified resource id if the
     * root resource id has not already been set. Otherwise, the existing root resource id is returned. This
     * method SHOULD NOT be called from any other operation other than write.
     *
     * @param headers the resource headers
     * @return the resolved root resource id
     */
    private String resolveRootResourceId(final ResourceHeaders headers) {
        if (rootResourceId == null) {
            rootResourceId = headers.getId();
            isArchivalGroup = headers.isArchivalGroup();
            isNewObject = true;
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
        ensureKnownRootResource();
        return rootResourceId;
    }

    private void ensureKnownRootResource() {
        if (rootResourceId == null) {
            throw new NotFoundException("No resource found in object " + ocflObjectId);
        }
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

    private String stagingPathToLogicalPath(final Path path) {
        final var relative = objectStaging.relativize(path).toString();

        if (SystemUtils.IS_OS_WINDOWS) {
            return URLDecoder.decode(relative.replace("\\", "/"), StandardCharsets.UTF_8);
        }

        return relative;
    }

    private Set<String> listCommittedHeaders() {
        if (!(isOpen() && deleteObject) && containsOcflObject()) {
            return ocflRepo.describeVersion(ObjectVersionId.head(ocflObjectId)).getFiles()
                    .stream()
                    .map(FileDetails::getPath)
                    .filter(PersistencePaths::isHeaderFile)
                    .collect(Collectors.toSet());
        }

        return Collections.emptySet();
    }

    private String resolveVersionNumber(final String resourceId, final String versionNumber) {
        if (versionNumber == null) {
            if (containsOcflObject()) {
                final var headVersion = ocflRepo.describeVersion(ObjectVersionId.head(ocflObjectId));
                return headVersion.getVersionNum().toString();
            }
            throw new NotFoundException(String.format("Resource %s was not found.", resourceId));
        }

        return versionNumber;
    }

    private void cleanup() {
        stagedHeaders.clear();
        stagedHeaderPaths.clear();
        deletePaths.clear();
        digests.clear();
        if (Files.exists(objectStaging)) {
            FileUtils.deleteQuietly(objectStaging.toFile());
        }
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
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String getOcflDigest(final Collection<URI> headerDigests) {
        if (headerDigests != null) {
            for (final var uri : headerDigests) {
                final var parts = uri.getSchemeSpecificPart().split(":");
                if (parts.length == 2 && digestAlgorithm.getJavaStandardName().equalsIgnoreCase(parts[0])) {
                    return parts[1];
                }
            }
        }
        return null;
    }

    private URI digestUri(final String digest) {
        try {
            return new URI("urn", digestAlgorithm.getJavaStandardName() + ":" + digest, null);
        } catch (final URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private DigestAlgorithm identifyObjectDigestAlgorithm() {
        if (containsOcflObject()) {
            return ocflRepo.describeObject(ocflObjectId).getDigestAlgorithm();
        } else {
            return ocflRepo.config().getDefaultDigestAlgorithm();
        }
    }

    private boolean hasMutableHeadAndShouldCreateNewVersion(final boolean hasMutableHead) {
        return commitType == CommitType.NEW_VERSION && hasMutableHead;
    }

    private void safeDelete(final Path path) {
        if (path != null) {
            try {
                Files.deleteIfExists(path);
            } catch (final IOException e) {
                LOG.error("Failed to delete staged file: {}", path);
            }
        }
    }

    private boolean containsOcflObject() {
        return ocflRepo.containsObject(ocflObjectId);
    }

    private void moveStagedHeadersToCache(final String newVersionNum) {
        stagedHeaders.forEach((id, headers) -> {
            addToCache(id, newVersionNum, headers);
        });
        stagedHeaders.clear();
    }

    /**
     * Adds a versioned copy of the resource's headers to the cache.
     *
     * Note, this cache is not invalidated on rollback or purge. This does not create bugs in Fedora because previous
     * versions of a resource are always accessed via mementos. This means that, for example, if a change is rolled
     * back, the resource headers from the change are left in the cache, but they are orphaned and will eventually
     * expire without being accessed again.
     *
     * @param resourceId the resource's id
     * @param versionNumber the version number of the resource
     * @param headers the headers
     */
    private void addToCache(final String resourceId,
                            final String versionNumber,
                            final ResourceHeaders headers) {
        final var key = cacheKey(resourceId, versionNumber);
        LOG.trace("Adding to cache {}", key);
        headersCache.put(key, headers);
    }

    private String cacheKey(final String id, final String versionNum) {
        return String.format("%s_%s", id, versionNum);
    }

    private NotFoundException notFoundException(final PathPair path, final String resourceId) {
        return new NotFoundException(String.format("File %s was not found for resource %s",
                path.path, resourceId));
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
