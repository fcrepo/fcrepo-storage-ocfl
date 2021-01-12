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
import org.fcrepo.storage.ocfl.ResourceHeaders;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author pwinckles
 */
public class ValidationUtilTest {

    private static final String ROOT_RESOURCE = "info:fedora";

    private static final Set<String> FORBIDDEN_PARTS = Set.of(
            "fcr-root",
            ".fcrepo",
            "fcr-container.nt",
            "fcr:tombstone",
            "fcr:versions"
    );

    private static final Set<String> FORBIDDEN_SUFFIXES = Set.of(
            "~fcr-desc",
            "~fcr-acl",
            "~fcr-desc.nt",
            "~fcr-acl.nt"
    );

    private static final Set<String> VALID_ALGORITHMS = Set.of(
            "sha-1", "sha1",
            "sha-256", "sha256",
            "sha-512", "sha512",
            "sha-512/256", "sha512/256",
            "md5"
    );

    private Context context;

    @Before
    public void setup() {
        context = new Context();
    }

    @Test
    public void validInteractionModels() {
        Arrays.stream(InteractionModel.values()).forEach(model -> {
            ValidationUtil.validateInteractionModel(context, model.getUri());
        });
        context.throwValidationException();
    }

    @Test
    public void validateModelFailsWhenNull() {
        ValidationUtil.validateInteractionModel(context, null);
        failsWith("Must define property 'interactionModel'");
    }

    @Test
    public void validateModelFailsWhenInvalid() {
        ValidationUtil.validateInteractionModel(context, "bogus");
        failsWith("Invalid interaction model value: bogus.");
    }

    @Test
    public void validateIdFailWhenNull() {
        ValidationUtil.validateId(context, "id", null);
        failsWith("Must define property 'id'");
    }

    @Test
    public void validateIdFailMissingPrefix() {
        ValidationUtil.validateId(context, "id", "asdf");
        failsWith("Invalid 'id' value 'asdf'. IDs must be prefixed with 'info:fedora/'");
    }

    @Test
    public void validateIdFailWhenEmptyPart() {
        ValidationUtil.validateId(context, "id", resourceId("a//b"));
        failsWith("Invalid 'id' value 'info:fedora/a//b'. IDs may not contain blank parts");
    }

    @Test
    public void validateIdFailWhenContainsIllegalParts() {
        final var tmpl = resourceId("a/%s/b");
        FORBIDDEN_PARTS.forEach(part -> {
            ValidationUtil.validateId(context, "id", String.format(tmpl, part));
        });
        failsWith("Invalid 'id' value 'info:fedora/a/fcr-root/b'. IDs may not contain parts equal to 'fcr-root'",
                "Invalid 'id' value 'info:fedora/a/fcr:versions/b'. IDs may not contain parts equal to 'fcr:versions'",
                "Invalid 'id' value 'info:fedora/a/.fcrepo/b'. IDs may not contain parts equal to '.fcrepo'",
                "Invalid 'id' value 'info:fedora/a/fcr-container.nt/b'. IDs may not contain parts equal" +
                        " to 'fcr-container.nt'",
                "Invalid 'id' value 'info:fedora/a/fcr:tombstone/b'. IDs may not contain parts equal to" +
                        " 'fcr:tombstone'");
    }

    @Test
    public void validateIdFailWhenContainsIllegalSuffix() {
        final var tmpl = resourceId("a/b/c%s");
        FORBIDDEN_SUFFIXES.forEach(part -> {
            ValidationUtil.validateId(context, "id", String.format(tmpl, part));
        });
        failsWith("Invalid 'id' value 'info:fedora/a/b/c~fcr-acl.nt'. IDs may not contain parts that end with '~fcr-acl.nt'",
                "Invalid 'id' value 'info:fedora/a/b/c~fcr-desc'. IDs may not contain parts that end with '~fcr-desc'",
                "Invalid 'id' value 'info:fedora/a/b/c~fcr-desc.nt'. IDs may not contain parts that end" +
                        " with '~fcr-desc.nt'",
                "Invalid 'id' value 'info:fedora/a/b/c~fcr-acl'. IDs may not contain parts that end with '~fcr-acl'");
    }

    @Test
    public void validateValidateId() {
        ValidationUtil.validateId(context, "id", resourceId("hello"));
        context.throwValidationException();
    }

    @Test
    public void validateRelatedIds() {
        ValidationUtil.validateIdRelationship(context, "parent", resourceId("foo"),
                "id", resourceId("foo/bar"));
        context.throwValidationException();
    }

    @Test
    public void validateRelatedIdsSame() {
        ValidationUtil.validateIdRelationship(context, "parent", resourceId("foo"),
                "id", resourceId("foo"));
        context.throwValidationException();
    }

    @Test
    public void validateRelatedIdsParentEndingInSlash() {
        ValidationUtil.validateIdRelationship(context, "parent", resourceId("foo/"),
                "id", resourceId("foo/bar/baz"));
        context.throwValidationException();
    }

    @Test
    public void validateRelatedIdsBothNull() {
        ValidationUtil.validateIdRelationship(context, "parent", null,
                "id", null);
        context.throwValidationException();
    }

    @Test
    public void validateRelatedWhenParentOnlyNull() {
        ValidationUtil.validateIdRelationship(context, "parent", null,
                "id", resourceId("foo"));
        context.throwValidationException();
    }

    @Test
    public void validateRelatedWhenChildOnlyNull() {
        ValidationUtil.validateIdRelationship(context, "parent", resourceId("foo"),
                "id", null);
        context.throwValidationException();
    }

    @Test
    public void failValidateRelatedWhenNotRelated() {
        ValidationUtil.validateIdRelationship(context, "parent", resourceId("foo"),
                "id", resourceId("bar"));
        failsWith("IDs that must be related: parent=info:fedora/foo; id=info:fedora/bar.");
    }

    @Test
    public void validateDigestsWhenValid() {
        final var tmpl = "urn:%s:digest";

        final var digests = new ArrayList<URI>();

        VALID_ALGORITHMS.forEach(algorithm -> {
            digests.add(URI.create(String.format(tmpl, algorithm)));
        });
        VALID_ALGORITHMS.stream().map(String::toUpperCase).forEach(algorithm -> {
            digests.add(URI.create(String.format(tmpl, algorithm)));
        });

        ValidationUtil.validateDigests(context, digests);
        context.throwValidationException();
    }

    @Test
    public void validateDigestWhenNullOrEmpty() {
        ValidationUtil.validateDigests(context, null);
        ValidationUtil.validateDigests(context, Collections.emptyList());
        context.throwValidationException();
    }

    @Test
    public void failValidateDigestWhenInvalid() {
        ValidationUtil.validateDigests(context, List.of(
                URI.create(""),
                URI.create("a:b:c"),
                URI.create("a:b"),
                URI.create("urn:bogus:asdf")
        ));
        failsWith("Digests must be formatted as 'urn:ALGORITHM:DIGEST'. Found: .",
                "Digests must begin with 'urn'. Found: a:b:c.",
                "Digest 'a:b:c' contains an invalid algorithm 'b'.",
                "Digests must be formatted as 'urn:ALGORITHM:DIGEST'. Found: a:b.",
                "Digest 'urn:bogus:asdf' contains an invalid algorithm 'bogus'.");
    }

    @Test
    public void failRequireNonNullWhenNull() {
        ValidationUtil.requireNotNull(context, "asdf", null);
        failsWith("Must define property 'asdf'");
    }

    @Test
    public void passRequireNonNullWhenNotNull() {
        ValidationUtil.requireNotNull(context, "asdf", "qwe");
        context.throwValidationException();
    }

    @Test
    public void failRequireNonEmptyWhenNull() {
        ValidationUtil.requireNotEmpty(context, "asdf", null);
        failsWith("Must define property 'asdf'");
    }

    @Test
    public void failRequireNonEmptyWhenEmpty() {
        ValidationUtil.requireNotEmpty(context, "asdf", Collections.emptyList());
        failsWith("Must contain a 'asdf' property with at least one entry.");
    }

    @Test
    public void passRequireNonEmptyWhenNotNull() {
        ValidationUtil.requireNotEmpty(context, "asdf", List.of("asdf"));
        context.throwValidationException();
    }

    @Test
    public void isModelTrueWhenMatches() {
        Arrays.stream(InteractionModel.values()).forEach(model -> {
            assertTrue(model.getUri(), ValidationUtil.isModel(model, model.getUri()));
        });
    }

    @Test
    public void isModelFalseWhenMatches() {
        assertFalse(ValidationUtil.isModel(InteractionModel.ACL, InteractionModel.DIRECT_CONTAINER.getUri()));
    }

    @Test
    public void contentNotExpectedWhenDeleted() {
        assertFalse(ValidationUtil.contentExpected(ResourceHeaders.builder()
                .withDeleted(true)
                .build()));
    }

    @Test
    public void contentNotExpectedWhenNotDeletedButExternal() {
        assertFalse(ValidationUtil.contentExpected(ResourceHeaders.builder()
                .withDeleted(false)
                .withExternalHandling("proxy")
                .build()));
    }

    @Test
    public void contentExpectedWhenNotDeletedOrExternal() {
        assertTrue(ValidationUtil.contentExpected(ResourceHeaders.builder()
                .withDeleted(false)
                .build()));
    }

    @Test
    public void isContainerTrueWheContainerTypeModel() {
        final var containers = Set.of(
                InteractionModel.DIRECT_CONTAINER,
                InteractionModel.BASIC_CONTAINER,
                InteractionModel.INDIRECT_CONTAINER);

        Arrays.stream(InteractionModel.values()).forEach(model -> {
            final var uri = model.getUri();
            if (containers.contains(model)) {
                assertTrue(uri, ValidationUtil.isContainer(uri));
            } else {
                assertFalse(uri, ValidationUtil.isContainer(uri));
            }
        });
    }

    private void failsWith(final String... problems) {
        if (context.getProblems().isEmpty()) {
            Assert.fail("Expected there to be problems: " + Arrays.asList(problems));
        }

        final var actualProblems = context.getProblems();
        final var actualNotFound = new HashSet<>(actualProblems);
        final var expectNotFound = new HashSet<>(Arrays.asList(problems));

        for (var expected : problems) {
            for (var actual : actualProblems) {
                if (actual.contains(expected)) {
                    expectNotFound.remove(expected);
                    actualNotFound.remove(actual);
                    break;
                }
            }
        }

        if (!expectNotFound.isEmpty()) {
            fail(String.format("Expected problems not found. Actual: %s; Missing: %s", actualProblems, expectNotFound));
        }
        if (!actualNotFound.isEmpty()) {
            fail(String.format("Unexpected problems found: %s", actualNotFound));
        }
    }

    private String resourceId(final String id) {
        return ROOT_RESOURCE + "/" + id;
    }

}
