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

package org.fcrepo.storage.ocfl.validation;

import com.fasterxml.jackson.databind.ObjectReader;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.model.ObjectDetails;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.OcflObjectVersion;
import edu.wisc.library.ocfl.api.model.VersionDetails;
import edu.wisc.library.ocfl.api.model.VersionNum;
import org.apache.commons.lang3.StringUtils;
import org.fcrepo.storage.ocfl.InteractionModel;
import org.fcrepo.storage.ocfl.PersistencePaths;
import org.fcrepo.storage.ocfl.ResourceHeaders;
import org.fcrepo.storage.ocfl.exception.ChecksumMismatchException;
import org.fcrepo.storage.ocfl.exception.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Validates that OCFL objects are correctly formatted to be used by Fedora 6
 *
 * @author pwinckles
 */
public class ObjectValidator {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectValidator.class);

    private final MutableOcflRepository ocflRepo;
    private final ObjectReader headerReader;
    private final HeadersValidator headersValidator;

    public ObjectValidator(final MutableOcflRepository ocflRepo,
                           final ObjectReader headerReader) {
        this.ocflRepo = Objects.requireNonNull(ocflRepo, "ocflRepo cannot be null");
        this.headerReader = Objects.requireNonNull(headerReader, "headerReader cannot be null");
        this.headersValidator = new HeadersValidator();
    }

    /**
     * Validates that the specified OCFL object is a valid Fedora 6 object
     *
     * @param ocflObjectId the ID of the OCFL object to validate
     * @param checkFixity true if file fixity should be validated in addition to the metadata validation
     * @throws ValidationException when the object fails validation
     */
    public void validate(final String ocflObjectId, final boolean checkFixity) {
        Objects.requireNonNull(ocflObjectId, "ocflObjectId cannot be null");

        LOG.debug("Validating object {} with fixity check {}", ocflObjectId, checkFixity);

        new ObjectValidatorInner(ocflObjectId, checkFixity).validate();
    }

    private class ObjectValidatorInner {
        private final String ocflObjectId;
        private final boolean checkFixity;

        private final Context context;
        private final Map<String, String> resourceModelMap;

        private ObjectValidatorInner(final String ocflObjectId, final boolean checkFixity) {
            this.ocflObjectId = ocflObjectId;
            this.checkFixity = checkFixity;
            this.context = new Context(ocflObjectId);
            this.resourceModelMap = new HashMap<>();
        }

        private void validate() {
            objectExists();
            final var objDetails = readInventory();

            objDetails.getVersionMap().forEach((versionNum, versionDetails) -> {
                try {
                    validateVersion(versionNum, versionDetails);
                } catch (ContinueException e) {
                    // Ignore -- this just means that a version is invalid
                    // but we still want to look at the other versions.
                } catch (RuntimeException e) {
                    context.problem("Unexpected failure validating version %s: %s", versionNum, e.getMessage());
                    LOG.warn("Unexpected failure validating version {}", versionNum, e);
                }
            });

            context.throwValidationException();
        }

        private void validateVersion(final VersionNum versionNum, final VersionDetails versionDetails) {
            LOG.debug("Validating object {} version {}", ocflObjectId, versionNum);

            final var resourceParentMap = new HashMap<String, String>();
            final var deletedResources = new HashSet<String>();
            final var nonRdfDescriptions = new HashSet<String>();
            final var nonRdfResources = new HashSet<String>();
            final var headerFiles = new HashSet<String>();
            final var contentFiles = new HashSet<String>();

            for (var file : versionDetails.getFiles()) {
                final var path = file.getPath();
                if (PersistencePaths.isHeaderFile(path)) {
                    headerFiles.add(path);
                } else {
                    contentFiles.add(path);
                }
            }

            final var unseenContentFiles = new HashSet<>(contentFiles);

            // the object's root resource headers must be present and parsable in order proceed with validation
            final var rootHeaders = validateRootHeaders(versionNum, headerFiles);

            headerFiles.forEach(headerFile -> {
                LOG.trace("Validating object {} version {} header file {}", ocflObjectId, versionNum, headerFile);

                try {
                    final var isRoot = PersistencePaths.ROOT_HEADER_PATH.equals(headerFile);
                    final var headers = isRoot ? rootHeaders : parseHeaders(versionNum, headerFile);

                    // Populates IDs in various data sets to be examined after validating the headers
                    if (headers.getId() != null) {
                        if (headers.isDeleted()) {
                            deletedResources.add(headers.getId());
                        }

                        // Ensure that the interaction model for the resource has not changed between versions
                        if (headers.getInteractionModel() != null) {
                            final var previousModel = resourceModelMap.get(headers.getId());
                            if (previousModel != null) {
                                if (!previousModel.equals(headers.getInteractionModel())) {
                                    context.problem("[Version %s header file %s] Interaction model declared as %s" +
                                                    " but a previous version declares the model as %s.",
                                            versionNum, headerFile, headers.getInteractionModel(), previousModel);
                                }
                            } else {
                                resourceModelMap.put(headers.getId(), headers.getInteractionModel());
                            }
                        }

                        if (headers.getParent() != null) {
                            resourceParentMap.put(headers.getId(), headers.getParent());
                        }

                        if (ValidationUtil.isModel(InteractionModel.NON_RDF_DESCRIPTION,
                                headers.getInteractionModel())) {
                            nonRdfDescriptions.add(headers.getId());
                        } else if (ValidationUtil.isModel(InteractionModel.NON_RDF, headers.getInteractionModel())) {
                            nonRdfResources.add(headers.getId());
                        }
                    }

                    final var persistencePaths = resolvePersistencePaths(versionNum, headerFile,
                            headers, rootHeaders);

                    if (persistencePaths != null) {
                        unseenContentFiles.remove(persistencePaths.getContentFilePath());

                        if (!headerFile.equals(persistencePaths.getHeaderFilePath())) {
                            context.problem("[Version %s header file %s] Header file should be located at %s",
                                    versionNum, headerFile, persistencePaths.getHeaderFilePath());
                        }
                    }

                    try {
                        headersValidator.validate(persistencePaths, headers, rootHeaders);
                    } catch (ValidationException e) {
                        e.getProblems().forEach(problem -> {
                            context.problem("[Version %s header file %s] %s", versionNum, headerFile, problem);
                        });
                    }

                    fixityCheck(versionNum, headerFile, persistencePaths, headers);
                } catch (ContinueException e) {
                    // Ignore -- this just means that a header file is invalid
                    // but we still want to look at the other files.
                } catch (RuntimeException e) {
                    context.problem("[Version %s header file %s] Unexpected failure: %s",
                            versionNum, headerFile, e.getMessage());
                    LOG.warn("Unexpected failure validating version {} header file {}",
                            versionNum, headerFile, e);
                }
            });

            validateNonRdfDescriptionsHaveNonRdfParent(versionNum, nonRdfDescriptions);
            validateNonRdfResourcesHaveDescription(versionNum, nonRdfResources);
            validateArchivalGroupParentage(versionNum, resourceParentMap, deletedResources, rootHeaders);

            unseenContentFiles.forEach(unseenFile -> {
                context.problem("[Version %s] Unexpected file: %s", versionNum, unseenFile);
            });
        }

        private ResourceHeaders validateRootHeaders(final VersionNum versionNum,
                                                    final Set<String> headerFiles) {
            final var headerFile = PersistencePaths.ROOT_HEADER_PATH;

            if (!headerFiles.contains(headerFile)) {
                context.problem("[Version %s] Missing root header file at %s",
                        versionNum, headerFile);
                throw new ContinueException();
            }

            final var headers = parseHeaders(versionNum, headerFile);

            final var idContext = new Context();
            ValidationUtil.validateId(idContext, "id", headers.getId());
            final var problem = idContext.getProblems().stream().findFirst().orElse(null);

            // Root resource ID MUST be valid
            if (problem != null) {
                context.problem("[Version %s header file %s] %s", versionNum, headerFile, problem);
                throw new ContinueException();
            }

            if (!Objects.equals(ocflObjectId, headers.getId())) {
                context.problem("[Version %s header file %s] Must define property 'id' as '%s' but was '%s'",
                        versionNum, headerFile, ocflObjectId, headers.getId());
            }

            return headers;
        }

        private void validateNonRdfDescriptionsHaveNonRdfParent(final VersionNum versionNum,
                                                                final Set<String> nonRdfDescriptions) {
            nonRdfDescriptions.forEach(id -> {
                final var parent = StringUtils.substringBeforeLast(id, "/");
                final var parentModel = resourceModelMap.get(parent);
                if (parentModel == null) {
                    context.problem("[Version %s resource %s] Non-RDF resource description without a corresponding" +
                                    " non-RDF resource.",
                            versionNum, id);
                } else if (!InteractionModel.NON_RDF.getUri().equals(parentModel)) {
                    context.problem("[Version %s resource %s] Must be the child of a non-RDF resource.",
                            versionNum, id);
                }
            });
        }

        private void validateNonRdfResourcesHaveDescription(final VersionNum versionNum,
                                                            final Set<String> nonRdfResources) {
            nonRdfResources.forEach(id -> {
                final var desc = id + "/" + PersistencePaths.FCR_METADATA;
                final var descModel = resourceModelMap.get(desc);
                if (descModel == null) {
                    context.problem("[Version %s resource %s] Non-RDF resource without a non-RDF description",
                            versionNum, id);
                } else if (!ValidationUtil.isModel(InteractionModel.NON_RDF_DESCRIPTION, descModel)) {
                    context.problem("[Version %s resource %s] Must have interaction model %s",
                            versionNum, desc, descModel, InteractionModel.NON_RDF_DESCRIPTION.getUri());
                }
            });
        }

        private void validateArchivalGroupParentage(final VersionNum versionNum,
                                                    final Map<String, String> resourceParentMap,
                                                    final Set<String> deletedResources,
                                                    final ResourceHeaders rootHeaders) {
            if (rootHeaders.isArchivalGroup()) {
                resourceParentMap.forEach((id, parent) -> {
                    if (!rootHeaders.getId().equals(id)) {
                        final var childDeleted = deletedResources.contains(id);
                        final var parentDeleted = deletedResources.contains(parent);

                        if (parentDeleted && !childDeleted) {
                            context.problem("[Version %s resource %s] Must be marked as deleted because parent" +
                                            " %s is marked as deleted.",
                                    versionNum, id, parent);
                        }

                        final var model = resourceModelMap.get(id);

                        // ACLs may be the child of any resource and non-RDF descriptions are covered elsewhere
                        if (model != null
                                && !ValidationUtil.isModel(InteractionModel.ACL, model)
                                && !ValidationUtil.isModel(InteractionModel.NON_RDF_DESCRIPTION, model)) {

                            if (resourceParentMap.containsKey(parent)) {
                                final var parentModel = resourceModelMap.get(parent);
                                if (parentModel != null) {
                                    if (!ValidationUtil.isContainer(parentModel)) {
                                        context.problem("[Version %s resource %s] Parent %s has interaction" +
                                                        " model %s, which cannot have children.",
                                                versionNum, id, parent, parentModel);
                                    }
                                }
                            } else {
                                context.problem("[Version %s resource %s] Parent %s does not exist.",
                                        versionNum, id, parent);
                            }
                        }
                    }
                });
            }
        }

        private void fixityCheck(final VersionNum versionNum,
                                 final String headerFile,
                                 final PersistencePaths persistencePaths,
                                 final ResourceHeaders headers) {
            if (checkFixity) {
                final var object = getObjectVersion(versionNum);

                fixityCheck(object, headerFile, null);
                if (persistencePaths != null && ValidationUtil.contentExpected(headers)) {
                    fixityCheck(object, persistencePaths.getContentFilePath(), headers.getDigests());
                }
            }
        }

        private void fixityCheck(final OcflObjectVersion object,
                                 final String path,
                                 final Collection<URI> expectedDigests) {
            LOG.debug("Fixity check object {} file {}", object.getObjectVersionId(), path);

            final var versionNum = object.getVersionNum();
            final var file = object.getFile(path);

            if (file != null) {
                try (final var stream = file.getStream()) {
                    if (expectedDigests != null) {
                        DigestUtil.checkFixity(stream, expectedDigests);
                    }

                    while (stream.read() != -1) {
                        // we don't care about the content -- just want the digest
                    }

                    stream.checkFixity();
                } catch (FixityCheckException e) {
                    context.problem("[Version %s file %s] Failed fixity check: %s",
                            versionNum, path, e.getMessage());
                } catch (ChecksumMismatchException e) {
                    e.getProblems().forEach(problem -> {
                        context.problem("[Version %s file %s] Failed fixity check: %s",
                                versionNum, path, problem);
                    });
                } catch (IOException | RuntimeException e) {
                    context.problem("[Version %s file %s] Failed to check fixity: %s",
                            versionNum, path, e.getMessage());
                }
            } else {
                context.problem("[Version %s file %s] Does not exist.",
                        versionNum, path);
            }
        }

        private ResourceHeaders parseHeaders(final VersionNum versionNum, final String headerFile) {
            final var object = getObjectVersion(versionNum);
            final var file = object.getFile(headerFile);

            try (final var stream = file.getStream()) {
                return headerReader.readValue(stream);
            } catch (IOException e) {
                context.problem("[Version %s header file %s] Failed to parse: %s",
                        versionNum, headerFile, e.getMessage());
                throw new ContinueException();
            }
        }

        private OcflObjectVersion getObjectVersion(final VersionNum versionNum) {
            try {
                return ocflRepo.getObject(ObjectVersionId.version(ocflObjectId, versionNum));
            } catch (RuntimeException e) {
                context.problem("OCFL object version %s could not be read: %s", versionNum, e.getMessage());
                throw new ContinueException();
            }
        }

        private PersistencePaths resolvePersistencePaths(final VersionNum versionNum,
                                                         final String headerFile,
                                                         final ResourceHeaders headers,
                                                         final ResourceHeaders rootHeaders) {
            PersistencePaths paths = null;
            final var resourceId = headers.getId();
            final var rootId = rootHeaders.getId();

            if (resourceId != null) {
                try {
                    if (ValidationUtil.isModel(InteractionModel.ACL, headers.getInteractionModel())
                            && headers.getParent() != null) {
                        final var parentHeadersPath = PersistencePaths.headerPath(rootId, headers.getParent());
                        final var parentHeaders = parseHeaders(versionNum, parentHeadersPath);
                        final var describesRdf = !ValidationUtil.isModel(InteractionModel.NON_RDF,
                                parentHeaders.getInteractionModel());
                        paths = PersistencePaths.aclResource(describesRdf, rootId, resourceId);
                    } else if (ValidationUtil.isModel(InteractionModel.NON_RDF, headers.getInteractionModel())) {
                        paths = PersistencePaths.nonRdfResource(rootId, resourceId);
                    } else if (headers.getInteractionModel() != null) {
                        paths = PersistencePaths.rdfResource(rootId, resourceId);
                    }
                } catch (RuntimeException e) {
                    context.problem("[Version %s header file %s] Unexpected problem identifying persistence paths: %s",
                            versionNum, headerFile, e.getMessage());
                }
            }

            return paths;
        }

        private void objectExists() {
            if (!ocflRepo.containsObject(ocflObjectId)) {
                context.problem("OCFL object does not exist in the repository");
                context.throwValidationException();
            }
        }

        private ObjectDetails readInventory() {
            try {
                return ocflRepo.describeObject(ocflObjectId);
            } catch (RuntimeException e) {
                context.problem("The OCFL object cannot be read: %s", e.getMessage());
                context.throwValidationException();
                // This code is not reachable
                throw new IllegalStateException();
            }
        }

    }

    /**
     * This exception is used to terminate a validation branch without terminating the entire validation
     */
    private static class ContinueException extends RuntimeException {

    }

}
