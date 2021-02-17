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

import org.apache.commons.lang3.StringUtils;
import org.fcrepo.storage.ocfl.InteractionModel;
import org.fcrepo.storage.ocfl.ResourceHeaders;

import java.net.URI;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * @author pwinckles
 */
final class ValidationUtil {

    private static final String INFO_FEDORA = "info:fedora";
    private static final String INFO_FEDORA_PREFIX = INFO_FEDORA + "/";

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

    private ValidationUtil() {

    }

    /**
     * Validates that the given interaction model is valid.
     *
     * @param context the context to add validation errors to
     * @param model the interaction model to validate
     */
    public static void validateInteractionModel(final Context context, final String model) {
        requireNotNull(context, "interactionModel", model);
        if (model != null) {
            try {
                InteractionModel.fromString(model);
            } catch (RuntimeException e) {
                context.problem("Invalid interaction model value: %s.", model);
            }
        }
    }

    /**
     * Validates that a string is a valid Fedora ID
     *
     * @param context the context to add validation errors to
     * @param name the property name the string came from, eg 'id', used in validation errors
     * @param value the id to validate
     */
    public static void validateId(final Context context, final String name, final String value) {
        requireNotNull(context, name, value);

        if (value != null) {
            if (!(INFO_FEDORA.equals(value) || value.startsWith(INFO_FEDORA_PREFIX))) {
                context.problem("Invalid '%s' value '%s'. IDs must be prefixed with '%s'",
                        name, value, INFO_FEDORA_PREFIX);
            }

            final var parts = value.split("/");

            for (var part : parts) {
                if (StringUtils.isBlank(part)) {
                    context.problem("Invalid '%s' value '%s'. IDs may not contain blank parts",
                            name, value);
                } else {
                    if (FORBIDDEN_PARTS.contains(part)) {
                        context.problem("Invalid '%s' value '%s'. IDs may not contain parts equal to '%s'",
                                name, value, part);
                    } else {
                        for (var suffix : FORBIDDEN_SUFFIXES) {
                            if (part.endsWith(suffix) && !part.equals(suffix)) {
                                context.problem("Invalid '%s' value '%s'. IDs may not contain parts that end with '%s'",
                                        name, value, suffix);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Validates that to Fedora IDs are related to each other. That is to say that one ID contains the other.
     * If either are null, the validation is skipped.
     *
     * @param context the context to add validation errors to
     * @param parentName the property name of the parent id
     * @param parentValue the value of the parent id
     * @param childName the property name of the child id
     * @param childValue the value of the child id
     */
    public static void validateIdRelationship(final Context context,
                                              final String parentName,
                                              final String parentValue,
                                              final String childName,
                                              final String childValue) {
        if (parentValue != null && childValue != null) {
            final var parentWithSlash = parentValue.endsWith("/") ? parentValue : parentValue + "/";
            if (!(childValue.equals(parentValue) || childValue.startsWith(parentWithSlash))) {
                context.problem("IDs must be related: %s=%s; %s=%s.",
                        parentName, parentValue, childName, childValue);
            }
        }
    }

    /**
     * Validates that a collection of digest URIs are in the expected format: urn:ALGORITHM:DIGEST
     *
     * @param context the context to add validation errors to
     * @param digests the digests to validate
     */
    public static void validateDigests(final Context context,
                                       final Collection<URI> digests) {

        if (digests != null && !digests.isEmpty()) {
            for (var digest : digests) {
                final var parts = digest.toString().split(":");

                if (parts.length != 3) {
                    context.problem("Digests must be formatted as 'urn:ALGORITHM:DIGEST'. Found: %s.",
                            digest);
                    continue;
                }

                if (!"urn".equals(parts[0])) {
                    context.problem("Digests must begin with 'urn'. Found: %s.", digest);
                }
                if (!VALID_ALGORITHMS.contains(parts[1].toLowerCase())) {
                    context.problem("Digest '%s' contains an invalid algorithm '%s'.", digest, parts[1]);
                }
                if (StringUtils.isBlank(parts[2])) {
                    context.problem("Digest '%s' is missing an expected digest value.", digest);
                }
            }
        }
    }

    /**
     * Validates that the given value is not null
     *
     * @param context the context to add validation errors to
     * @param name the value's property name
     * @param value the value
     */
    public static void requireNotNull(final Context context, final String name, final Object value) {
        if (value == null) {
            context.problem("Must define property '%s'", name);
        }
    }

    /**
     * Validates that the give value equals an expected value
     *
     * @param context the context to add validation errors to
     * @param name the value's property name
     * @param expected the expected value
     * @param actual the actual value
     * @param <T> the value's type
     */
    public static <T> void requireValue(final Context context, final String name, final T expected, final T actual) {
        if (!Objects.equals(expected, actual)) {
            context.problem("Must define property '%s' as '%s' but was '%s'", name, expected, actual);
        }
    }

    /**
     * Validates that the given collections is not null or empty
     *
     * @param context the context to add validation errors to
     * @param name the collection's property name
     * @param collection the collection
     */
    public static void requireNotEmpty(final Context context, final String name, final Collection<?> collection) {
        requireNotNull(context, name, collection);
        if (collection != null && collection.isEmpty()) {
            context.problem("Must contain a '%s' property with at least one entry.", name);
        }
    }

    /**
     * Returns true if the interaction model is equal to the expected model
     *
     * @param expected the expected model
     * @param actual the actual model
     * @return true when equal
     */
    public static boolean isModel(final InteractionModel expected, final String actual) {
        return expected.getUri().equals(actual);
    }

    /**
     * Returns true when there should be a content file
     *
     * @param headers the resource headers
     * @return true if there should be a content file
     */
    public static boolean contentExpected(final ResourceHeaders headers) {
        return !headers.isDeleted() && headers.getExternalHandling() == null;
    }

    /**
     * Returns true if the given model is a container type
     *
     * @param model the interaction model
     * @return true if it is some type of container
     */
    public static boolean isContainer(final String model) {
        return isModel(InteractionModel.BASIC_CONTAINER, model) ||
                isModel(InteractionModel.DIRECT_CONTAINER, model) ||
                isModel(InteractionModel.INDIRECT_CONTAINER, model);
    }

}
