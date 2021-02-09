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

import org.fcrepo.storage.ocfl.InteractionModel;
import org.fcrepo.storage.ocfl.PersistencePaths;
import org.fcrepo.storage.ocfl.ResourceHeaders;
import org.fcrepo.storage.ocfl.ResourceUtils;
import org.fcrepo.storage.ocfl.exception.ValidationException;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;

/**
 * @author pwinckles
 */
public class HeadersValidatorTest {

    private static final String ROOT_RESOURCE = "info:fedora";

    private HeadersValidator validator;

    private String defaultId;
    private String defaultAclId;
    private String defaultDescId;

    private long count;

    @Before
    public void setup() {
        validator = new HeadersValidator();
        defaultId = ResourceUtils.resourceId(UUID.randomUUID().toString());
        defaultAclId = ResourceUtils.toAclId(defaultId);
        defaultDescId = ResourceUtils.toDescId(defaultId);
        count = 0;
    }

    @Test
    public void validAtomicNonRdfResource() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah").getHeaders();
        validator.validate(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers);
    }

    @Test
    public void validAtomicNonRdfDescriptionResource() {
        final var rootHeaders = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah").getHeaders();
        final var headers = ResourceUtils.atomicDesc(defaultDescId, defaultId, "blah desc").getHeaders();
        validator.validate(PersistencePaths.rdfResource(defaultId, defaultDescId), headers, rootHeaders);
    }

    @Test
    public void validAtomicRdfResource() {
        final var headers = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah").getHeaders();
        validator.validate(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers);
    }

    @Test
    public void validAtomicDirectContainer() {
        final var headers = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withInteractionModel(InteractionModel.DIRECT_CONTAINER.getUri());
        }).getHeaders();
        validator.validate(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers);
    }

    @Test
    public void validAtomicIndirectContainer() {
        final var headers = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withInteractionModel(InteractionModel.INDIRECT_CONTAINER.getUri());
        }).getHeaders();
        validator.validate(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers);
    }

    @Test
    public void validArchivalGroupBasicContainerEmpty() {
        final var headers = ResourceUtils.ag(defaultId, ROOT_RESOURCE, "blah").getHeaders();
        validator.validate(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers);
    }

    @Test
    public void validArchivalGroupIndirectContainerEmpty() {
        final var headers = ResourceUtils.ag(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withInteractionModel(InteractionModel.INDIRECT_CONTAINER.getUri());
        }).getHeaders();
        validator.validate(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers);
    }

    @Test
    public void validArchivalGroupDirectContainerEmpty() {
        final var headers = ResourceUtils.ag(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withInteractionModel(InteractionModel.DIRECT_CONTAINER.getUri());
        }).getHeaders();
        validator.validate(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers);
    }

    @Test
    public void validArchivalGroupPartRdfChild() {
        final var childId = child(defaultId);

        final var agHeaders = ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag").getHeaders();
        final var childHeaders = ResourceUtils.partContainer(childId, defaultId, defaultId, "rdf").getHeaders();

        validator.validate(PersistencePaths.rdfResource(defaultId, childId), childHeaders, agHeaders);
    }

    @Test
    public void validArchivalGroupPartNonRdfChild() {
        final var childId = child(defaultId);

        final var agHeaders = ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag").getHeaders();
        final var childHeaders = ResourceUtils.partBinary(childId, defaultId, defaultId, "bin").getHeaders();

        validator.validate(PersistencePaths.nonRdfResource(defaultId, childId), childHeaders, agHeaders);
    }


    @Test
    public void validAtomicNonRdfResourceAcl() {
        final var rootHeaders = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah").getHeaders();
        final var aclHeaders = ResourceUtils.atomicBinaryAcl(defaultAclId, defaultId, "blah acl").getHeaders();

        validator.validate(PersistencePaths.aclResource(false, defaultId, defaultAclId), aclHeaders, rootHeaders);
    }

    @Test
    public void validAtomicRdfResourceAcl() {
        final var rootHeaders = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah").getHeaders();
        final var aclHeaders = ResourceUtils.atomicContainerAcl(defaultAclId, defaultId, "blah acl").getHeaders();

        validator.validate(PersistencePaths.aclResource(true, defaultId, defaultAclId), aclHeaders, rootHeaders);
    }


    @Test
    public void validDeletedAtomicResource() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withDeleted(true).withContentPath(null).withContentSize(-1);
        }).getHeaders();
        validator.validate(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers);
    }

    @Test
    public void validDeletedArchivalGroup() {
        final var childId = child(defaultId);

        final var agHeaders = ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag", h -> {
            h.withDeleted(true).withContentPath(null).withContentSize(-1);
        }).getHeaders();
        final var childHeaders = ResourceUtils.partBinary(childId, defaultId, defaultId, "bin", h -> {
            h.withDeleted(true).withContentPath(null).withContentSize(-1);
        }).getHeaders();

        validator.validate(PersistencePaths.nonRdfResource(defaultId, childId), childHeaders, agHeaders);
    }

    @Test
    public void failWhenIdInvalid() {
        final var headers = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withId("bogus");
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers,
                containsString("Invalid 'id' value 'bogus'."));
    }

    @Test
    public void failWhenParentInvalid() {
        final var headers = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withParent("bogus");
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers,
                containsString("Invalid 'parent' value 'bogus'."));
    }

    @Test
    public void failWhenParentChildDoNotMatch() {
        final var headers = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withParent("info:fedora/bogus");
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers,
                containsString("IDs must be related: parent=info:fedora/bogus; id=" + defaultId));
    }

    @Test
    public void failWhenStateTokenMissing() {
        final var headers = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withStateToken(null);
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers,
                containsString("Must define property 'stateToken'"));
    }

    @Test
    public void failWhenInteractionModelMissing() {
        final var headers = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withInteractionModel(null);
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers,
                containsString("Must define property 'interactionModel'"));
    }

    @Test
    public void failWhenInvalidInteractionModel() {
        final var headers = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withInteractionModel("bogus");
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers,
                containsString("Invalid interaction model value: bogus."));
    }

    @Test
    public void failWhenMissingCreateDate() {
        final var headers = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withCreatedDate(null);
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers,
                containsString("Must define property 'createdDate'"));
    }

    @Test
    public void failWhenMissingLastModifiedDate() {
        final var headers = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withLastModifiedDate(null);
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers,
                containsString("Must define property 'lastModifiedDate'"));
    }

    @Test
    public void failWhenMissingRequiredContent() {
        final var headers = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withContentPath(null);
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers,
                containsString("Must define property 'contentPath' as 'fcr-container.nt' but was 'null'"));
    }

    @Test
    public void passWhenMissingContentExternal() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withContentPath(null).withExternalHandling("proxy").withExternalUrl("http://www.example.com");
        }).getHeaders();
        validator.validate(PersistencePaths.nonRdfResource(defaultId, defaultAclId), headers, headers);
    }

    @Test
    public void failWhenRootDeletedButChildNot() {
        final var childId = child(defaultId);

        final var agHeaders = ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag", h -> {
            h.withDeleted(true);
        }).getHeaders();
        final var headers = ResourceUtils.partContainer(childId, defaultId, defaultId, "blah").getHeaders();

        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, childId), headers, agHeaders,
                containsString("Must define property 'deleted' as 'true' but was 'false'"));
    }

    @Test
    public void failWhenMissingRequiredDigests() {
        final var headers = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withDigests(Collections.emptyList());
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers,
                containsString("Must contain a 'digests' property with at least one entry."));
    }

    @Test
    public void failWhenDigestsNotRequiredButContainsInvalidDigest() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withExternalHandling("proxy").withExternalUrl("http://www.example.com")
                    .withDigests(List.of(URI.create("urn:bogus:asdf")));
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers,
                containsString("Digest 'urn:bogus:asdf' contains an invalid algorithm 'bogus'."));
    }

    @Test
    public void failWhenNonRdfMissingMime() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withMimeType(null);
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers,
                containsString("Must define property 'mimeType'"));
    }

    @Test
    public void passWhenNonRdfDeletedMissingMime() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withMimeType(null).withDeleted(true);
        }).getHeaders();
        validator.validate(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers);
    }

    @Test
    public void passWhenRdfMissingMime() {
        final var headers = ResourceUtils.atomicContainer(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withMimeType(null);
        }).getHeaders();
        validator.validate(PersistencePaths.rdfResource(defaultId, defaultId), headers, headers);
    }

    @Test
    public void failWhenExternalMissingUrl() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withExternalHandling("proxy");
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers,
                containsString("Must define property 'externalUrl'"));
    }

    @Test
    public void failWhenExternalMissingHandling() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withExternalUrl("http://www.example.com");
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers,
                containsString("Must define property 'externalHandling' as one of"));
    }

    @Test
    public void failWhenExternalInvalidHandling() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withExternalUrl("http://www.example.com").withExternalHandling("blah");
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers,
                containsString("Must define property 'externalHandling' as one of"));
    }

    @Test
    public void failWhenExternalWrongModel() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withExternalUrl("http://www.example.com").withExternalHandling("redirect")
                    .withInteractionModel(InteractionModel.BASIC_CONTAINER.getUri());
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers,
                containsString("Must define property 'interactionModel' as 'http://www.w3.org/ns/ldp#NonRDFSource'" +
                        " but was 'http://www.w3.org/ns/ldp#BasicContainer'"));
    }

    @Test
    public void failWhenRootResourceAclModel() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withInteractionModel(InteractionModel.ACL.getUri());
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers,
                containsString("Invalid interaction model value: http://fedora.info/definitions/v4/webac#Acl."));
    }

    @Test
    public void failWhenRootResourceDescriptionModel() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withInteractionModel(InteractionModel.NON_RDF_DESCRIPTION.getUri());
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers,
                containsString("Invalid interaction model value:" +
                        " http://fedora.info/definitions/v4/repository#NonRdfSourceDescription."));
    }

    @Test
    public void failWhenRootResourceButRootPropFalse() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withObjectRoot(false);
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers,
                containsString("Must define property 'objectRoot' as 'true' but was 'false'"));
    }

    @Test
    public void failWhenRootResourceAndAgIdSet() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withArchivalGroupId(defaultId);
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers,
                containsString("Must define property 'archivalGroupId' as 'null' but was "));
    }

    @Test
    public void failWhenAgButNotContainer() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withArchivalGroup(true);
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers,
                containsString("Archival Group has an invalid interaction model:" +
                        " http://www.w3.org/ns/ldp#NonRDFSource"));
    }

    @Test
    public void failWhenPartMarkedAsRoot() {
        final var childId = child(defaultId);

        final var agHeaders = ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag").getHeaders();
        final var headers = ResourceUtils.partContainer(childId, defaultId, defaultId, "blah", h -> {
            h.withObjectRoot(true);
        }).getHeaders();

        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, childId), headers, agHeaders,
                containsString("Must define property 'objectRoot' as 'false' but was 'true'"));
    }

    @Test
    public void failWhenPartMarkedAsAg() {
        final var childId = child(defaultId);

        final var agHeaders = ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag").getHeaders();
        final var headers = ResourceUtils.partContainer(childId, defaultId, defaultId, "blah", h -> {
            h.withArchivalGroup(true);
        }).getHeaders();

        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, childId), headers, agHeaders,
                containsString("Must define property 'archivalGroup' as 'false' but was 'true'"));
    }

    @Test
    public void failWhenPartMissingAgId() {
        final var childId = child(defaultId);

        final var agHeaders = ResourceUtils.ag(defaultId, ROOT_RESOURCE, "ag").getHeaders();
        final var headers = ResourceUtils.partContainer(childId, defaultId, defaultId, "blah", h -> {
            h.withArchivalGroupId(null);
        }).getHeaders();

        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, childId), headers, agHeaders,
                containsString("Must define property 'archivalGroupId' as '" + defaultId + "' but was 'null'"));
    }

    @Test
    public void failAtomicPartWhenNonRdf() {
        final var rootHeaders = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "bin").getHeaders();
        final var headers = ResourceUtils.atomicDesc(defaultDescId, defaultId, "desc", h -> {
            h.withInteractionModel(InteractionModel.NON_RDF.getUri());
        }).getHeaders();

        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, defaultDescId), headers, rootHeaders,
                containsString("Invalid interaction model http://www.w3.org/ns/ldp#NonRDFSource." +
                        " Atomic resources may only contain ACLs and non-RDF descriptions."));
    }

    @Test
    public void failAtomicPartWhenRdf() {
        final var rootHeaders = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "bin").getHeaders();
        final var headers = ResourceUtils.atomicDesc(defaultDescId, defaultId, "desc", h -> {
            h.withInteractionModel(InteractionModel.BASIC_CONTAINER.getUri());
        }).getHeaders();

        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, defaultDescId), headers, rootHeaders,
                containsString("Invalid interaction model http://www.w3.org/ns/ldp#BasicContainer." +
                        " Atomic resources may only contain ACLs and non-RDF descriptions."));
    }

    @Test
    public void failAtomicPartWhenAgReference() {
        final var rootHeaders = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "bin").getHeaders();
        final var headers = ResourceUtils.atomicDesc(defaultDescId, defaultId, "desc", h -> {
            h.withArchivalGroupId(defaultId);
        }).getHeaders();

        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, defaultDescId), headers, rootHeaders,
                containsString("Must define property 'archivalGroupId' as 'null'"));
    }

    @Test
    public void failAtomicPartDescWhenIncorrectId() {
        final var id = child(defaultId);

        final var rootHeaders = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "bin").getHeaders();
        final var headers = ResourceUtils.atomicDesc(id, defaultId, "desc").getHeaders();

        expectFailureRelaxed(PersistencePaths.rdfResource(defaultId, defaultDescId), headers, rootHeaders,
                containsString("Must define property 'id' as '" + defaultId + "/fcr:metadata' but was '" + id));
    }

    @Test
    public void failAtomicPartAclWhenIncorrectId() {
        final var id = child(defaultId);

        final var rootHeaders = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "bin").getHeaders();
        final var headers = ResourceUtils.atomicAcl(false, defaultAclId, defaultId, "acl", h -> {
            h.withId(id);
        }).getHeaders();

        expectFailureRelaxed(PersistencePaths.aclResource(false, defaultId, defaultAclId), headers, rootHeaders,
                containsString("Must define property 'id' as '" + defaultId + "/fcr:acl' but was '" + id));
    }

    @Test
    public void failWhenHeadersVersionNotSet() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withHeadersVersion(null);
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers,
                containsString("Must define property 'headersVersion' as '1.0' but was 'null'"));
    }

    @Test
    public void failWhenHeadersVersionInvalid() {
        final var headers = ResourceUtils.atomicBinary(defaultId, ROOT_RESOURCE, "blah", h -> {
            h.withHeadersVersion("2.5");
        }).getHeaders();
        expectFailureRelaxed(PersistencePaths.nonRdfResource(defaultId, defaultId), headers, headers,
                containsString("Must define property 'headersVersion' as '1.0' but was '2.5'"));
    }

    @SafeVarargs
    private void expectFailureRelaxed(final PersistencePaths paths,
                                      final ResourceHeaders headers,
                                      final ResourceHeaders rootHeaders,
                                      final Matcher<String>... matchers) {
        try {
            validator.validate(paths, headers, rootHeaders);
            fail("Expected header validation to fail but it did not");
        } catch (ValidationException e) {
            assertThat(e.getProblems(), Matchers.hasItems(matchers));
        }
    }

    private String child(final String id) {
        return id + "/" + count++;
    }

}
