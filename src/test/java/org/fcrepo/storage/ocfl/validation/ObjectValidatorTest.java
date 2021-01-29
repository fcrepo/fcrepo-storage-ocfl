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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import edu.wisc.library.ocfl.core.path.mapper.LogicalPathMappers;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.fcrepo.storage.ocfl.CommitType;
import org.fcrepo.storage.ocfl.DefaultOcflObjectSessionFactory;
import org.fcrepo.storage.ocfl.InteractionModel;
import org.fcrepo.storage.ocfl.OcflObjectSessionFactory;
import org.fcrepo.storage.ocfl.PersistencePaths;
import org.fcrepo.storage.ocfl.ResourceContent;
import org.fcrepo.storage.ocfl.ResourceHeaders;
import org.fcrepo.storage.ocfl.ResourceUtils;
import org.fcrepo.storage.ocfl.cache.NoOpCache;
import org.fcrepo.storage.ocfl.exception.ValidationException;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.fail;

/**
 * @author pwinckles
 */
public class ObjectValidatorTest {

    private static final String ROOT_RESOURCE = "info:fedora";
    private static final String DEFAULT_MESSAGE = "F6 migration";
    private static final String DEFAULT_USER = "fedoraAdmin";
    private static final String DEFAULT_ADDRESS = "info:fedora/fedoraAdmin";

    @Rule
    public TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    private Path ocflRoot;

    private MutableOcflRepository ocflRepo;
    private OcflObjectSessionFactory sessionFactory;
    private ObjectMapper objectMapper;

    private ObjectValidator objectValidator;

    private long count;

    private String defaultId;
    private String defaultAclId;
    private String defaultDescId;

    @Before
    public void setup() throws IOException {
        ocflRoot = temp.newFolder("ocfl").toPath();
        final var sessionStaging = temp.newFolder("staging").toPath();
        final var ocflTemp = temp.newFolder("ocfl-temp").toPath();

        final var logicalPathMapper = SystemUtils.IS_OS_WINDOWS ?
                LogicalPathMappers.percentEncodingWindowsMapper() : LogicalPathMappers.percentEncodingLinuxMapper();

        ocflRepo = new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .logicalPathMapper(logicalPathMapper)
                .storage(FileSystemOcflStorage.builder().repositoryRoot(ocflRoot).build())
                .workDir(ocflTemp)
                .buildMutable();

        objectMapper = new ObjectMapper()
                .configure(WRITE_DATES_AS_TIMESTAMPS, false)
                .registerModule(new JavaTimeModule())
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);

        sessionFactory = new DefaultOcflObjectSessionFactory(ocflRepo,
                sessionStaging,
                objectMapper,
                new NoOpCache<>(),
                CommitType.NEW_VERSION, DEFAULT_MESSAGE, DEFAULT_USER, DEFAULT_ADDRESS);

        objectValidator = new ObjectValidator(ocflRepo, objectMapper.readerFor(ResourceHeaders.class));

        count = 0;
        defaultId = ResourceUtils.resourceId(UUID.randomUUID().toString());
        defaultAclId =  ResourceUtils.toAclId(defaultId);
        defaultDescId =  ResourceUtils.toDescId(defaultId);
    }

    @Test
    public void validAtomicNonRdfResource() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah"),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void validAtomicRdfResource() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah"));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void validAtomicDirectContainer() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", headers -> {
                    headers.withInteractionModel(InteractionModel.DIRECT_CONTAINER.getUri());
                }));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void validAtomicIndirectContainer() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", headers -> {
                    headers.withInteractionModel(InteractionModel.INDIRECT_CONTAINER.getUri());
                }));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void validArchivalGroupBasicContainerEmpty() {
        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, "blah"));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void validArchivalGroupIndirectContainerEmpty() {
        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, "blah", headers -> {
                    headers.withInteractionModel(InteractionModel.INDIRECT_CONTAINER.getUri());
                }));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void validArchivalGroupDirectContainerEmpty() {
        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, "blah", headers -> {
                    headers.withInteractionModel(InteractionModel.DIRECT_CONTAINER.getUri());
                }));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void validArchivalGroupContainingRdfAndNonRdfChildren() {
        final var binChildId = child(defaultId);
        final var rdfChildId = child(defaultId);
        final var binGrandChildId = child(rdfChildId);

        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag"),

                ResourceUtils.partBinary(binChildId, defaultId, defaultId, "bin1"),
                ResourceUtils.partDesc(ResourceUtils.toDescId(binChildId), binChildId, defaultId, "bin1 desc"),

                ResourceUtils.partContainer(rdfChildId, defaultId, defaultId, "rdf"),

                ResourceUtils.partBinary(binGrandChildId, rdfChildId, defaultId, "bin2"),
                ResourceUtils.partDesc(ResourceUtils.toDescId(binGrandChildId), binGrandChildId, defaultId,
                        "bin2 desc"));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void validAtomicNonRdfResourceWithAcl() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah"),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"),
                ResourceUtils.atomicBinaryAcl(defaultAclId, defaultId, "blah acl"));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void validAtomicRdfResourceWithAcl() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah"),
                ResourceUtils.atomicContainerAcl(defaultAclId, defaultId, "blah acl"));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void validArchivalGroupWithChildrenAndAcls() {
        final var binChildId = child(defaultId);
        final var rdfChildId = child(defaultId);
        final var binGrandChildId = child(rdfChildId);

        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag"),
                ResourceUtils.partContainerAcl(defaultAclId, defaultId, defaultId, "ag acl"),

                ResourceUtils.partBinary(binChildId, defaultId, defaultId, "bin1"),
                ResourceUtils.partDesc(ResourceUtils.toDescId(binChildId), binChildId, defaultId, "bin1 desc"),
                ResourceUtils.partBinaryAcl(ResourceUtils.toAclId(binChildId), binChildId, defaultId, "bin1 acl"),

                ResourceUtils.partContainer(rdfChildId, defaultId, defaultId, "rdf"),
                ResourceUtils.partContainerAcl(ResourceUtils.toAclId(rdfChildId), rdfChildId, defaultId, "rdf acl"),

                ResourceUtils.partBinary(binGrandChildId, rdfChildId, defaultId, "bin2"),
                ResourceUtils.partDesc(ResourceUtils.toDescId(binGrandChildId), binGrandChildId, defaultId,
                        "bin2 desc"),
                ResourceUtils.partBinaryAcl(ResourceUtils.toAclId(binGrandChildId), binGrandChildId, defaultId,
                        "bin2 acl"));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void validDeletedAtomicResource() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah"),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"));

        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, null, headers -> {
                    headers.withDeleted(true);
                }),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, null, headers -> {
                    headers.withDeleted(true);
                }));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void validArchivalGroupContainingDeletedResource() {
        final var binChildId = child(defaultId);
        final var rdfChildId = child(defaultId);
        final var binGrandChildId = child(rdfChildId);

        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag"),

                ResourceUtils.partBinary(binChildId, defaultId, defaultId, "bin1"),
                ResourceUtils.partDesc(ResourceUtils.toDescId(binChildId), binChildId, defaultId, "bin1 desc"),

                ResourceUtils.partContainer(rdfChildId, defaultId, defaultId, "rdf"),

                ResourceUtils.partBinary(binGrandChildId, rdfChildId, defaultId, "bin2"),
                ResourceUtils.partDesc(ResourceUtils.toDescId(binGrandChildId), binGrandChildId, defaultId,
                        "bin2 desc"));

        writeAndCommit(defaultId,
                ResourceUtils.partContainer(rdfChildId, defaultId, defaultId, null, headers -> {
                    headers.withDeleted(true);
                }),

                ResourceUtils.partBinary(binGrandChildId, rdfChildId, defaultId, null, headers -> {
                    headers.withDeleted(true);
                }),
                ResourceUtils.partDesc(ResourceUtils.toDescId(binGrandChildId), binGrandChildId, defaultId,
                        null, headers -> {
                    headers.withDeleted(true);
                }));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void validDeletedArchivalGroup() {
        final var binChildId = child(defaultId);
        final var rdfChildId = child(defaultId);
        final var binGrandChildId = child(rdfChildId);

        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag"),

                ResourceUtils.partBinary(binChildId, defaultId, defaultId, "bin1"),
                ResourceUtils.partDesc(ResourceUtils.toDescId(binChildId), binChildId, defaultId, "bin1 desc"),

                ResourceUtils.partContainer(rdfChildId, defaultId, defaultId, "rdf"),

                ResourceUtils.partBinary(binGrandChildId, rdfChildId, defaultId, "bin2"),
                ResourceUtils.partDesc(ResourceUtils.toDescId(binGrandChildId), binGrandChildId, defaultId,
                        "bin2 desc"));

        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, null, headers -> {
                    headers.withDeleted(true);
                }),

                ResourceUtils.partBinary(binChildId, defaultId, defaultId, null, headers -> {
                    headers.withDeleted(true);
                }),
                ResourceUtils.partDesc(ResourceUtils.toDescId(binChildId), binChildId, defaultId, null, headers -> {
                    headers.withDeleted(true);
                }),

                ResourceUtils.partContainer(rdfChildId, defaultId, defaultId, null, headers -> {
                    headers.withDeleted(true);
                }),

                ResourceUtils.partBinary(binGrandChildId, rdfChildId, defaultId, null, headers -> {
                    headers.withDeleted(true);
                }),
                ResourceUtils.partDesc(ResourceUtils.toDescId(binGrandChildId), binGrandChildId, defaultId, null,
                        headers -> {
                    headers.withDeleted(true);
                }));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void failWhenObjectDoesNotExist() {
        validationFailureStrict(defaultId, true,
                containsString("does not exist in the repository"));
    }

    @Test
    public void failWhenVersionDoesNotContainRootHeaders() throws IOException {
        ocflRepo.updateObject(ObjectVersionId.version(defaultId, 0), null, updater -> {
            updater.writeFile(IOUtils.toInputStream("blah", StandardCharsets.UTF_8), "blah");
        });

        validationFailureStrict(defaultId, true,
                containsString("[Version v1] Missing root header file at .fcrepo/fcr-root.json"));
    }

    @Test
    public void failWhenHeadersNotParsable() throws IOException {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah"));

        final var headersPath = storagePath(defaultId, 1, PersistencePaths.ROOT_HEADER_PATH);
        Files.writeString(headersPath, "blah", StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);

        validationFailureStrict(defaultId, true,
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Failed to parse"));
    }

    @Test
    public void failWhenRootHeadersMissingId() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah"));

        final var headersPath = storagePath(defaultId, 1, PersistencePaths.ROOT_HEADER_PATH);
        modifyHeaders(headersPath, headers -> {
            headers.withId(null);
        });

        validationFailureStrict(defaultId, true,
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Must define property 'id'"));
    }

    @Test
    public void failWhenRootHeadersHasInvalidId() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah"));

        final var headersPath = storagePath(defaultId, 1, PersistencePaths.ROOT_HEADER_PATH);
        modifyHeaders(headersPath, headers -> {
            headers.withId("bogus");
        });

        validationFailureStrict(defaultId, true,
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Invalid 'id' value 'bogus'." +
                        " IDs must be prefixed with 'info:fedora/'"));
    }

    @Test
    public void failWhenRootHeadersMissingInteractionModel() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah"));

        final var headersPath = storagePath(defaultId, 1, PersistencePaths.ROOT_HEADER_PATH);
        modifyHeaders(headersPath, headers -> {
            headers.withInteractionModel(null);
        });

        validationFailureRelaxed(defaultId, false,
                containsString("[Version v1 header file .fcrepo/fcr-root.json]" +
                        " Must define property 'interactionModel'"));
    }

    @Test
    public void failWhenRootHeadersHasInvalidInteractionModel() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah"));

        final var headersPath = storagePath(defaultId, 1, PersistencePaths.ROOT_HEADER_PATH);
        modifyHeaders(headersPath, headers -> {
            headers.withInteractionModel("bogus");
        });

        validationFailureStrict(defaultId, false,
                containsString("[Version v1 header file .fcrepo/fcr-root.json]" +
                        " Invalid interaction model value: bogus."));
    }

    @Test
    public void failWhenRootHeadersHasAclInteractionModel() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah"));

        final var headersPath = storagePath(defaultId, 1, PersistencePaths.ROOT_HEADER_PATH);
        modifyHeaders(headersPath, headers -> {
            headers.withInteractionModel(InteractionModel.ACL.getUri());
        });

        validationFailureRelaxed(defaultId, false,
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Invalid interaction" +
                        " model value: http://fedora.info/definitions/v4/webac#Acl."));
    }

    @Test
    public void failWhenRootHeadersHasNonRdfDescInteractionModel() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah"));

        final var headersPath = storagePath(defaultId, 1, PersistencePaths.ROOT_HEADER_PATH);
        modifyHeaders(headersPath, headers -> {
            headers.withInteractionModel(InteractionModel.NON_RDF_DESCRIPTION.getUri());
        });

        validationFailureRelaxed(defaultId, false,
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Invalid interaction" +
                        " model value: http://fedora.info/definitions/v4/repository#NonRdfSourceDescription."));
    }

    @Test
    public void failWhenRootHeadersIncorrectBasicHeaders() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah"));

        final var headersPath = storagePath(defaultId, 1, PersistencePaths.ROOT_HEADER_PATH);
        modifyHeaders(headersPath, headers -> {
            headers.withId("info:fedora/bogus")
                    .withObjectRoot(false)
                    .withArchivalGroupId(ROOT_RESOURCE);
        });

        validationFailureStrict(defaultId, false,
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Must define property 'id' as '"
                        + defaultId + "' but was 'info:fedora/bogus'"),
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Must define property 'objectRoot'" +
                        " as 'true' but was 'false'"),
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Must define property" +
                        " 'archivalGroupId' as 'null' but was 'info:fedora'"));
    }

    @Test
    public void failWhenAgButWrongModel() {
        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, "blah"));

        final var headersPath = storagePath(defaultId, 1, PersistencePaths.ROOT_HEADER_PATH);
        modifyHeaders(headersPath, headers -> {
            headers.withInteractionModel(InteractionModel.NON_RDF.getUri());
        });

        validationFailureRelaxed(defaultId, true,
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Archival Group has an" +
                        " invalid interaction model: http://www.w3.org/ns/ldp#NonRDFSource"));
    }

    @Test
    public void failWhenInteractionModelChanges() {
        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, "blah"));

        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah"),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"));

        validationFailureRelaxed(defaultId, false,
                containsString("[Version v2 header file .fcrepo/fcr-root.json] Interaction model declared" +
                        " as http://www.w3.org/ns/ldp#NonRDFSource but a previous version declares the" +
                        " model as http://www.w3.org/ns/ldp#BasicContainer."));
    }

    @Test
    public void failWhenMissingBasicHeaders() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah"),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"));

        final var headersPath = storagePath(defaultId, 1,
                PersistencePaths.headerPath(defaultId, defaultDescId));
        modifyHeaders(headersPath, headers -> {
            headers.withId(null)
                    .withParent(null)
                    .withInteractionModel(null)
                    .withStateToken(null)
                    .withCreatedDate(null)
                    .withLastModifiedDate(null)
                    .withDigests(null);
        });

        validationFailureRelaxed(defaultId, false,
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json]" +
                        " Must define property 'createdDate'"),
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json]" +
                        " Must define property 'lastModifiedDate'"),
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json]" +
                        " Must define property 'interactionModel'"),
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json]" +
                        " Must contain a 'digests' property with at" +
                        " least one entry."),
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json]" +
                        " Must define property 'stateToken'"),
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json]" +
                        " Must define property 'parent'"),
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json]" +
                        " Must define property 'interactionModel'"),
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json]" +
                        " Invalid interaction model null." +
                        " Atomic resources may only contain ACLs and non-RDF descriptions."));
    }

    @Test
    public void failWhenChildAndParentAreUnrelated() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah"),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"));

        final var headersPath = storagePath(defaultId, 1,
                PersistencePaths.headerPath(defaultId, defaultDescId));
        modifyHeaders(headersPath, headers -> {
            headers.withId(ResourceUtils.resourceId("orphan"));
        });

        validationFailureRelaxed(defaultId, false,
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json] IDs" +
                        " must be related: parent=" + defaultId + "; id=info:fedora/orphan."),
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json] Must define property 'id' as" +
                                " '" + defaultDescId + "' but was 'info:fedora/orphan'"));
    }

    @Test
    public void failWhenHeaderInWrongLocation() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah"),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"));

        ocflRepo.updateObject(ObjectVersionId.head(defaultId), null, updater -> {
            updater.renameFile(PersistencePaths.headerPath(defaultId, defaultDescId), ".fcrepo/headers.json");
        });

        validationFailureStrict(defaultId, true,
                containsString("[Version v2 header file .fcrepo/headers.json] Header file" +
                        " should be located at .fcrepo/fcr-root~fcr-desc.json"));
    }

    @Test
    public void failWhenRootDeletedButChildNot() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", headers -> {
                    headers.withDeleted(true);
                }),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"));

        validationFailureStrict(defaultId, true,
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json]" +
                        " Must define property 'deleted' as 'true' but was 'false'"));
    }

    @Test
    public void failWhenHasInvalidDigests() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", headers -> {
                    headers.withDigests(List.of(
                            URI.create(""),
                            URI.create("bogus:md5:blah"),
                            URI.create("urn:sha1"),
                            URI.create("urn:sha100:asdf")
                    ));
                }));

        validationFailureStrict(defaultId, false,
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Digests must" +
                        " be formatted as 'urn:ALGORITHM:DIGEST'. Found: ."),
                containsString("Version v1 header file .fcrepo/fcr-root.json] Digests must begin with 'urn'." +
                        " Found: bogus:md5:blah."),
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Digests must be formatted as" +
                        " 'urn:ALGORITHM:DIGEST'. Found: urn:sha1."),
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Digest 'urn:sha100:asdf' contains an" +
                        " invalid algorithm 'sha100'."));
    }

    @Test
    public void failWhenNonRdfHeadersMissingMimeType() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", headers -> {
                    headers.withMimeType(null);
                }),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"));

        validationFailureStrict(defaultId, true,
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Must define" +
                        " property 'mimeType'"));
    }

    @Test
    public void passWhenRdfHeadersMissingMimeType() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", headers -> {
                    headers.withMimeType(null);
                }));

        objectValidator.validate(defaultId, true);
    }

    @Test
    public void failWhenExternalHandlingSpecifiedButNotUrl() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", headers -> {
                    headers.withExternalHandling("proxy")
                            .withExternalUrl(null)
                            .withContentPath(null);
                }),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"));

        validationFailureStrict(defaultId, true,
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Must define" +
                        " property 'externalUrl'"));
    }

    @Test
    public void failWhenExternalUrlSpecifiedButNotHandling() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", headers -> {
                    headers.withExternalHandling(null)
                            .withExternalUrl("https://github.com/fcrepo/fcrepo")
                            .withContentPath(null);
                }),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"));

        validationFailureStrict(defaultId, true,
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Must define property" +
                        " 'externalHandling' as one of "));
    }

    @Test
    public void failWhenInvalidExternalHandling() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", headers -> {
                    headers.withExternalHandling("copy")
                            .withExternalUrl("https://github.com/fcrepo/fcrepo")
                            .withContentPath(null);
                }),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"));

        validationFailureStrict(defaultId, true,
                containsString("[Version v1 header file .fcrepo/fcr-root.json] Must define property" +
                        " 'externalHandling' as one of "));
    }

    @Test
    public void failWhenExternalResourceWithWrongModel() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", headers -> {
                    headers.withExternalHandling("redirect")
                            .withExternalUrl("https://github.com/fcrepo/fcrepo");
                }),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"));

        final var headersPath = storagePath(defaultId, 1,
                PersistencePaths.ROOT_HEADER_PATH);
        modifyHeaders(headersPath, headers -> {
            headers.withInteractionModel(InteractionModel.BASIC_CONTAINER.getUri());
        });

        validationFailureRelaxed(defaultId, false,
                containsString("[Version v1 header file .fcrepo/fcr-root.json]" +
                        " Must define property 'interactionModel' as 'http://www.w3.org/ns/ldp#NonRDFSource'" +
                        " but was 'http://www.w3.org/ns/ldp#BasicContainer'"));
    }

    @Test
    public void failWhenParResourceHasInvalidHeaders() {
        final var rdfChildId = child(defaultId);

        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag"),
                ResourceUtils.partContainer(rdfChildId, defaultId, defaultId, "rdf"));

        final var headersPath = storagePath(defaultId, 1,
                PersistencePaths.headerPath(defaultId, rdfChildId));
        modifyHeaders(headersPath, headers -> {
            headers.withObjectRoot(true)
                    .withArchivalGroup(true)
                    .withArchivalGroupId(null);
        });

        validationFailureStrict(defaultId, false,
                containsString("[Version v1 header file .fcrepo/0.json] Must define property" +
                        " 'objectRoot' as 'false' but was 'true'"),
                containsString("[Version v1 header file .fcrepo/0.json] Must define property 'archivalGroup'" +
                        " as 'false' but was 'true'"),
                containsString("[Version v1 header file .fcrepo/0.json] Must define property 'archivalGroupId' as '"
                        + defaultId + "' but was 'null'"));
    }

    @Test
    public void failWhenAtomicBinaryDescHasInvalidHeaders() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah"),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"));

        final var headersPath = storagePath(defaultId, 1,
                PersistencePaths.headerPath(defaultId, defaultDescId));
        modifyHeaders(headersPath, headers -> {
            headers.withObjectRoot(true)
                    .withArchivalGroup(true)
                    .withArchivalGroupId(defaultId);
        });

        validationFailureStrict(defaultId, false,
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json]" +
                        " Must define property 'objectRoot' as 'false' but was 'true'"),
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json] Must define property" +
                        " 'archivalGroup' as 'false' but was 'true'"),
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json] Must define property" +
                        " 'archivalGroupId' as 'null' but was '" + defaultId + "'"));
    }

    @Test
    public void failWhenAtomicBinaryAclHasInvalidHeaders() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah"),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc"),
                ResourceUtils.atomicBinaryAcl(defaultAclId, defaultId, "blah acl"));

        final var headersPath = storagePath(defaultId, 1,
                PersistencePaths.headerPath(defaultId, defaultAclId));
        modifyHeaders(headersPath, headers -> {
            headers.withObjectRoot(true)
                    .withArchivalGroup(true)
                    .withArchivalGroupId(defaultId);
        });

        validationFailureStrict(defaultId, false,
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-acl.json]" +
                        " Must define property 'objectRoot' as 'false' but was 'true'"),
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-acl.json] Must define property" +
                        " 'archivalGroup' as 'false' but was 'true'"),
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-acl.json] Must define property" +
                        " 'archivalGroupId' as 'null' but was '" + defaultId + "'"));
    }

    @Test
    public void failWhenAtomicWithParts() {
        final var childId = child(defaultId);
        final var childId2 = child(defaultId);

        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah"),
                ResourceUtils.atomicBinary(childId, defaultId, "child"),
                ResourceUtils.atomicContainer(childId2, defaultId, "child2"));

        validationFailureRelaxed(defaultId, false,
                containsString("[Version v1 header file .fcrepo/0.json] Invalid interaction model" +
                        " http://www.w3.org/ns/ldp#NonRDFSource. Atomic resources may only contain ACLs" +
                        " and non-RDF descriptions."),
                containsString("[Version v1 header file .fcrepo/1.json] Invalid interaction model" +
                        " http://www.w3.org/ns/ldp#BasicContainer. Atomic resources may only contain" +
                        " ACLs and non-RDF descriptions."));
    }

    @Test
    public void failWhenAtomicWithAclAndDescAtWrongId() {
        final var bogusAclId = child(defaultId);
        final var bogusDescId = child(defaultId);

        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah"),
                ResourceUtils.atomicBinaryAcl(defaultAclId, defaultId, "child"),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "child2"));

        final var aclHeaders = storagePath(defaultId, 1,
                PersistencePaths.headerPath(defaultId, defaultAclId));
        modifyHeaders(aclHeaders, headers -> {
            headers.withId(bogusAclId);
        });
        final var descHeaders = storagePath(defaultId, 1,
                PersistencePaths.headerPath(defaultId, defaultDescId));
        modifyHeaders(descHeaders, headers -> {
            headers.withId(bogusDescId);
        });

        validationFailureRelaxed(defaultId, false,
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-desc.json]" +
                        " Must define property 'id' as '" + defaultDescId + "' but was '" + bogusDescId + "'"),
                containsString("[Version v1 header file .fcrepo/fcr-root~fcr-acl.json] Must define property 'id' as '" +
                        defaultAclId + "' but was '" + bogusAclId + "'"));
    }

    @Test
    public void failWhenHeadersFailFixityCheck() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah"));

        final var headersPath = storagePath(defaultId, 1,
                PersistencePaths.ROOT_HEADER_PATH);
        modifyHeaders(headersPath, headers -> {
            headers.withContentSize(100L);
        });

        validationFailureStrict(defaultId, true,
                containsString("[Version v1 file .fcrepo/fcr-root.json] Failed fixity check:" +
                        " Expected sha-512 digest: "));
    }

    @Test
    public void failWhenContentFailsFixityCheck() throws IOException {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah"));

        final var contentPath = storagePath(defaultId, 1,
                PersistencePaths.rdfResource(defaultId, defaultId).getContentFilePath());
        Files.writeString(contentPath, "changed!", StandardOpenOption.TRUNCATE_EXISTING);

        validationFailureStrict(defaultId, true,
                containsString("[Version v1 file fcr-container.nt] Failed fixity check:" +
                        " SHA-512 fixity check failed. Expected: "));
    }

    @Test
    public void failWhenDescHasRdfParent() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah"),
                ResourceUtils.atomicDesc(defaultDescId, defaultId, "desc"));

        validationFailureStrict(defaultId, true,
                containsString("[Version v1 resource " + defaultDescId + "] Must be the" +
                        " child of a non-RDF resource."));
    }

    @Test
    public void failWhenNonRdfHasNoDescription() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah"));

        validationFailureStrict(defaultId, true,
                containsString("[Version v1 resource " + defaultId + "]" +
                        " Non-RDF resource without a non-RDF description"));
    }

    @Test
    public void failWhenPartHasParentThatDoesNotExist() {
        final var childId = child(defaultId);
        final var grandChildId = child(childId);

        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag"),
                ResourceUtils.partContainer(grandChildId, childId, defaultId, "rdf"));

        validationFailureStrict(defaultId, true,
                containsString("[Version v1 resource " + grandChildId + "] Parent "
                        + childId + " does not exist."));
    }

    @Test
    public void failWhenPartHasParentThatIsNonRdf() {
        final var childId = child(defaultId);
        final var grandChildId = child(childId);

        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag"),
                ResourceUtils.partContainer(childId, defaultId, defaultId, "bin"),
                ResourceUtils.partContainer(grandChildId, childId, defaultId, "rdf"));

        final var headersPath = storagePath(defaultId, 1,
                PersistencePaths.headerPath(defaultId, childId));
        modifyHeaders(headersPath, headers -> {
            headers.withInteractionModel(InteractionModel.NON_RDF.getUri());
        });

        validationFailureRelaxed(defaultId, false,
                containsString("[Version v1 resource " + grandChildId + "] Parent "
                        + childId + " has interaction" +
                        " model http://www.w3.org/ns/ldp#NonRDFSource, which cannot have children."));
    }

    @Test
    public void failWhenParentDeletedAndChildNot() {
        final var childId = child(defaultId);
        final var grandChildId = child(childId);

        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag"),
                ResourceUtils.partContainer(childId, defaultId, defaultId, "rdf", headers -> {
                    headers.withDeleted(true);
                }),
                ResourceUtils.partContainer(grandChildId, childId, defaultId, "bin"));

        validationFailureStrict(defaultId, false,
                containsString("[Version v1 resource " + grandChildId + "] Must be marked as deleted" +
                        " because parent " + childId + " is marked as deleted."));
    }

    @Test
    public void failWhenHasUnexpectedFiles() {
        writeAndCommit(defaultId,
                ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah"));

        ocflRepo.updateObject(ObjectVersionId.head(defaultId), null, updater -> {
            updater.writeFile(IOUtils.toInputStream("surprise!", StandardCharsets.UTF_8), "secrets.txt");
        });

        validationFailureStrict(defaultId, true,
                containsString("[Version v2] Unexpected file: secrets.txt"));
    }

    @Test
    public void validationShouldReportFailuresFromMultipleVersions() throws IOException {
        final var binChildId = child(defaultId);
        final var rdfChildId = child(defaultId);
        final var binGrandChildId = child(rdfChildId);
        final var binGrandChildDescId =  ResourceUtils.toDescId(binGrandChildId);

        writeAndCommit(defaultId,
                ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag"),

                ResourceUtils.partBinary(binChildId, defaultId, defaultId, "bin1"),
                ResourceUtils.partDesc(ResourceUtils.toDescId(binChildId), binChildId, defaultId, "bin1 desc"),

                ResourceUtils.partContainer(rdfChildId, defaultId, defaultId, "rdf"),

                ResourceUtils.partBinary(binGrandChildId, rdfChildId, defaultId, "bin2"),
                ResourceUtils.partDesc(binGrandChildDescId, binGrandChildId, defaultId, "bin2 desc"));

        final var invalidChildId = child(rdfChildId);

        writeAndCommit(defaultId,
                ResourceUtils.partContainer(invalidChildId, binGrandChildId, defaultId, "rdf"));

        final var atomicChildId = child(defaultId);

        writeAndCommit(defaultId,
                ResourceUtils.atomicBinary(atomicChildId, defaultId, "blah"));

        final var contentPath = storagePath(defaultId, 1,
                PersistencePaths.rdfResource(defaultId, binGrandChildDescId).getContentFilePath());

        Files.delete(contentPath);

        validationFailureStrict(defaultId, true,
                containsString("[Version v1 file 1/2~fcr-desc.nt] Failed to check fixity:" +
                        " NoSuchFileException:"),
                containsString("[Version v2 file 1/2~fcr-desc.nt] Failed to check fixity: NoSuchFileException:"),
                containsString("[Version v2 header file .fcrepo/1/3.json] IDs must be related: parent="
                        + binGrandChildId + "; id=" + invalidChildId + "."),
                containsString("[Version v2 resource " + invalidChildId + "] Parent " + binGrandChildId
                        + " has interaction model http://www.w3.org/ns/ldp#NonRDFSource, which cannot have children."),
                containsString("[Version v3 header file .fcrepo/4.json] Must define property 'objectRoot' as" +
                        " 'false' but was 'true'"),
                containsString("[Version v3 header file .fcrepo/4.json] Must define property 'archivalGroupId' as '"
                        + defaultId + "' but was 'null'"),
                containsString("[Version v3 file 1/2~fcr-desc.nt] Failed to check fixity: NoSuchFileException:"),
                containsString("[Version v3 header file .fcrepo/1/3.json] IDs must be related: parent="
                        + binGrandChildId + "; id=" + invalidChildId + "."),
                containsString("[Version v3 resource " + atomicChildId + "] Non-RDF resource without a" +
                        " non-RDF description"),
                containsString("[Version v3 resource " + invalidChildId + "] Parent "
                        + binGrandChildId + " has interaction model http://www.w3.org/ns/ldp#NonRDFSource," +
                        " which cannot have children."));
    }

    @SafeVarargs
    private void validationFailureRelaxed(final String id,
                                          final boolean checkFixity,
                                          final Matcher<String>... expectedProblems) {
        validationFailure(id, checkFixity, false, expectedProblems);
    }

    @SafeVarargs
    private void validationFailureStrict(final String id,
                                         final boolean checkFixity,
                                         final Matcher<String>... expectedProblems) {
        validationFailure(id, checkFixity, true, expectedProblems);
    }

    @SafeVarargs
    private void validationFailure(final String id,
                                   final boolean checkFixity,
                                   final boolean matchAll,
                                   final Matcher<String>... expectedProblems) {
        try {
            objectValidator.validate(id, checkFixity);
            fail(String.format("Expected validation of %s to fail with problems: %s",
                    id, Arrays.asList(expectedProblems)));
        } catch (ValidationException e) {
            if (matchAll) {
                assertThat(e.getProblems(), containsInAnyOrder(expectedProblems));
            } else {
                assertThat(e.getProblems(), hasItems(expectedProblems));
            }
        }
    }

    private void writeAndCommit(final String id, final ResourceContent... content) {
        final var session = sessionFactory.newSession(id);
        for (var c : content) {
            session.writeResource(c.getHeaders(), c.getContentStream().orElse(null));
        }
        session.commit();
    }

    private void modifyHeaders(final Path path, final Consumer<ResourceHeaders.Builder> modifyHeaders) {
        try {
            final var headers = objectMapper.readValue(path.toFile(), ResourceHeaders.class);
            final var builder = ResourceHeaders.builder(headers);

            modifyHeaders.accept(builder);

            objectMapper.writeValue(path.toFile(), builder.build());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path storagePath(final String id, final int versionNum, final String logicalPath) {
        final var file = ocflRepo.describeVersion(ObjectVersionId.version(id, versionNum)).getFile(logicalPath);
        if (file == null) {
            throw new RuntimeException(String.format("File %s not found in object %s version %s",
                    logicalPath, id, versionNum));
        }
        return ocflRoot.resolve(file.getStorageRelativePath());
    }

    private String child(final String id) {
        return id + "/" + count++;
    }

}
