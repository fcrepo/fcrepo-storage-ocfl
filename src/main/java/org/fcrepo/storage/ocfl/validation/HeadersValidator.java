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
import org.fcrepo.storage.ocfl.exception.ValidationException;

import java.util.Objects;
import java.util.Set;

/**
 * @author pwinckles
 */
public class HeadersValidator {

    private static final Set<String> VALID_EXT_HANDLING = Set.of("proxy", "redirect");

    /**
     * Validates resource headers. The root headers MUST have a valid id and this method MUST NOT
     * be called without first validating it.
     *
     * @param paths the persistence paths for the resource, may be null
     * @param headers the headers to validate, may not be null
     * @param rootHeaders the headers for the resource at the root of the OCFL object, may not be null
     * @throws ValidationException when problems are identified
     */
    public void validate(final PersistencePaths paths,
                         final ResourceHeaders headers,
                         final ResourceHeaders rootHeaders) {
        Objects.requireNonNull(headers, "headers may not be null");
        Objects.requireNonNull(rootHeaders, "rootHeaders may not be null");

        final var context = new Context();

        generalHeaderValidation(context, paths, headers, rootHeaders);
        nonRdfHeaderValidation(context, headers);

        if (!Objects.equals(headers.getId(), rootHeaders.getId())) {
            ValidationUtil.requireValue(context, "objectRoot", false, headers.isObjectRoot());
            ValidationUtil.requireValue(context, "archivalGroup", false, headers.isArchivalGroup());

            if (rootHeaders.isArchivalGroup()) {
                validateArchivalPartHeaders(context, headers, rootHeaders);
            } else {
                validateAtomicPartHeaders(context, headers);
            }
        } else {
            validateRootHeaders(context, headers);
        }

        context.throwValidationException();
    }

    private void validateRootHeaders(final Context context, final ResourceHeaders headers) {
        final var model = headers.getInteractionModel();

        if (ValidationUtil.isModel(InteractionModel.ACL, model)
                || ValidationUtil.isModel(InteractionModel.NON_RDF_DESCRIPTION, model)) {
            context.problem("Invalid interaction model value: %s.", model);
        }

        ValidationUtil.requireValue(context, "objectRoot", true, headers.isObjectRoot());
        ValidationUtil.requireValue(context, "archivalGroupId", null, headers.getArchivalGroupId());

        if (headers.isArchivalGroup() && !ValidationUtil.isContainer(model)) {
            context.problem("Archival Group has an invalid interaction model: %s", model);
        }
    }

    private void generalHeaderValidation(final Context context,
                                         final PersistencePaths paths,
                                         final ResourceHeaders headers,
                                         final ResourceHeaders rootHeaders) {
        ValidationUtil.validateId(context, "id", headers.getId());
        ValidationUtil.validateId(context, "parent", headers.getParent());
        ValidationUtil.validateIdRelationship(context, "parent", headers.getParent(),
                "id", headers.getId());
        ValidationUtil.validateIdRelationship(context, "rootId", rootHeaders.getId(),
                "id", headers.getId());

        ValidationUtil.requireNotNull(context, "stateToken", headers.getStateToken());
        ValidationUtil.validateInteractionModel(context, headers.getInteractionModel());
        ValidationUtil.requireNotNull(context, "createdDate", headers.getCreatedDate());
        ValidationUtil.requireNotNull(context, "lastModifiedDate", headers.getLastModifiedDate());

        final var contentExpected = ValidationUtil.contentExpected(headers);

        if (paths != null && contentExpected) {
            ValidationUtil.requireValue(context, "contentPath", paths.getContentFilePath(), headers.getContentPath());
        }

        if (rootHeaders.isDeleted()) {
            ValidationUtil.requireValue(context, "deleted", rootHeaders.isDeleted(), headers.isDeleted());
        }

        // All resources that are not external or deleted must have content files
        if (contentExpected) {
            ValidationUtil.requireNotEmpty(context, "digests", headers.getDigests());
        }

        ValidationUtil.validateDigests(context, headers.getDigests());
    }

    private void nonRdfHeaderValidation(final Context context, final ResourceHeaders headers) {
        if (!headers.isDeleted() && ValidationUtil.isModel(InteractionModel.NON_RDF, headers.getInteractionModel())) {
            ValidationUtil.requireNotNull(context, "mimeType", headers.getMimeType());
        }

        if (headers.getExternalHandling() != null || headers.getExternalUrl() != null) {
            ValidationUtil.requireNotNull(context, "externalUrl", headers.getExternalUrl());

            if (headers.getExternalHandling() == null
                    || !VALID_EXT_HANDLING.contains(headers.getExternalHandling())) {
                context.problem("Must define property 'externalHandling' as one of %s", VALID_EXT_HANDLING);
            }

            ValidationUtil.requireValue(context, "interactionModel",
                    InteractionModel.NON_RDF.getUri(), headers.getInteractionModel());
        }
    }

    private void validateArchivalPartHeaders(final Context context,
                                             final ResourceHeaders headers,
                                             final ResourceHeaders rootHeaders) {
        ValidationUtil.requireValue(context, "archivalGroupId", rootHeaders.getId(), headers.getArchivalGroupId());
    }

    private void validateAtomicPartHeaders(final Context context, final ResourceHeaders headers) {
        final var model = headers.getInteractionModel();

        if (!(ValidationUtil.isModel(InteractionModel.ACL, model)
                || ValidationUtil.isModel(InteractionModel.NON_RDF_DESCRIPTION, model))) {
            context.problem("Invalid interaction model %s." +
                            " Atomic resources may only contain ACLs and non-RDF descriptions.", model);
        } else if (headers.getParent() != null) {
            if (ValidationUtil.isModel(InteractionModel.ACL, model)) {
                ValidationUtil.requireValue(context, "id", headers.getParent() + "/" + PersistencePaths.FCR_ACL,
                        headers.getId());
            } else {
                ValidationUtil.requireValue(context, "id", headers.getParent() + "/" + PersistencePaths.FCR_METADATA,
                        headers.getId());
            }
        }

        ValidationUtil.requireValue(context, "archivalGroupId", null, headers.getArchivalGroupId());
    }

}
