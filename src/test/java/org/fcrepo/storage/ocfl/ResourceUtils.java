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

import org.apache.commons.codec.digest.DigestUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.function.Consumer;

/**
 * @author pwinckles
 */
public final class ResourceUtils {

    private static final String ROOT_RESOURCE = "info:fedora";
    public static final String DEFAULT_USER = "fedoraAdmin";
    public static final String RDF_MIME = "text/turtle";
    public static final String TEXT_MIME = "text/plain";

    private ResourceUtils() {

    }

    public static String resourceId(final String id) {
        return ROOT_RESOURCE + "/" + id;
    }

    public static String toDescId(final String id) {
        return id + "/fcr:metadata";
    }

    public static String toAclId(final String id) {
        return id + "/fcr:acl";
    }

    public static ResourceContent atomicBinary(final String resourceId, final String parentId, final String content) {
        return atomicBinary(resourceId, parentId, content, null);
    }

    public static ResourceContent atomicBinary(final String resourceId,
                                         final String parentId,
                                         final String content,
                                         final Consumer<ResourceHeaders.Builder> modifyHeaders) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.withObjectRoot(true);
        headers.withArchivalGroup(false);
        headers.withInteractionModel(InteractionModel.NON_RDF.getUri());
        headers.withMimeType(TEXT_MIME);

        if (content != null) {
            headers.withContentPath(PersistencePaths.nonRdfResource(resourceId, resourceId).getContentFilePath());
        }

        if (modifyHeaders != null) {
            modifyHeaders.accept(headers);
        }

        return new ResourceContent(stream(content), headers.build());
    }

    public static ResourceContent partBinary(final String resourceId,
                                             final String parentId,
                                             final String rootResourceId,
                                             final String content) {
        return partBinary(resourceId, parentId, rootResourceId, content, null);
    }

    public static ResourceContent partBinary(final String resourceId,
                                             final String parentId,
                                             final String rootResourceId,
                                             final String content,
                                             final Consumer<ResourceHeaders.Builder> modifyHeaders) {
        return atomicBinary(resourceId, parentId, content, headers -> {
            headers.withObjectRoot(false);
            headers.withArchivalGroupId(rootResourceId);
            headers.withContentPath(PersistencePaths.nonRdfResource(rootResourceId, resourceId).getContentFilePath());
            if (modifyHeaders != null) {
                modifyHeaders.accept(headers);
            }
        });
    }

    public static ResourceContent atomicContainer(final String resourceId,
                                                  final String parentId,
                                                  final String content) {
        return atomicContainer(resourceId, parentId, content, null);
    }

    public static ResourceContent atomicContainer(final String resourceId,
                                                  final String parentId,
                                                  final String content,
                                                  final Consumer<ResourceHeaders.Builder> modifyHeaders) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.withObjectRoot(true);
        headers.withArchivalGroup(false);
        headers.withInteractionModel(InteractionModel.BASIC_CONTAINER.getUri());
        headers.withMimeType(RDF_MIME);

        if (content != null) {
            headers.withContentPath(PersistencePaths.rdfResource(resourceId, resourceId).getContentFilePath());
        }

        if (modifyHeaders != null) {
            modifyHeaders.accept(headers);
        }

        return new ResourceContent(stream(content), headers.build());
    }

    public static ResourceContent partContainer(final String resourceId,
                                                final String parentId,
                                                final String rootResourceId,
                                                final String content) {
        return partContainer(resourceId, parentId, rootResourceId, content, null);
    }

    public static ResourceContent partContainer(final String resourceId,
                                                final String parentId,
                                                final String rootResourceId,
                                                final String content,
                                                final Consumer<ResourceHeaders.Builder> modifyHeaders) {
        return atomicContainer(resourceId, parentId, content, headers -> {
            headers.withObjectRoot(false);
            headers.withArchivalGroupId(rootResourceId);

            if (content != null) {
                headers.withContentPath(PersistencePaths.rdfResource(rootResourceId, resourceId).getContentFilePath());
            }

            if (modifyHeaders != null) {
                modifyHeaders.accept(headers);
            }
        });
    }

    public static ResourceContent ag(final String resourceId,
                                     final String parentId,
                                     final String content) {
        return ag(resourceId, parentId, content, null);
    }

    public static ResourceContent ag(final String resourceId,
                                     final String parentId,
                                     final String content,
                                     final Consumer<ResourceHeaders.Builder> modifyHeaders) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.withObjectRoot(true);
        headers.withArchivalGroup(true);
        headers.withInteractionModel(InteractionModel.BASIC_CONTAINER.getUri());
        headers.withMimeType(RDF_MIME);

        if (content != null) {
            headers.withContentPath(PersistencePaths.rdfResource(resourceId, resourceId).getContentFilePath());
        }

        if (modifyHeaders != null) {
            modifyHeaders.accept(headers);
        }

        return new ResourceContent(stream(content), headers.build());
    }

    public static ResourceContent atomicContainerAcl(final String resourceId,
                                                     final String parentId,
                                                     final String content) {
        return atomicAcl(true, resourceId, parentId, content, null);
    }

    public static ResourceContent atomicBinaryAcl(final String resourceId,
                                                  final String parentId,
                                                  final String content) {
        return atomicAcl(false, resourceId, parentId, content, null);
    }

    public static ResourceContent atomicAcl(final boolean describesRdf,
                                            final String resourceId,
                                            final String parentId,
                                            final String content,
                                            final Consumer<ResourceHeaders.Builder> modifyHeaders) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.withObjectRoot(false);
        headers.withArchivalGroup(false);
        headers.withInteractionModel(InteractionModel.ACL.getUri());
        headers.withMimeType(RDF_MIME);

        if (content != null) {
            headers.withContentPath(PersistencePaths.aclResource(describesRdf, parentId, resourceId)
                    .getContentFilePath());
        }

        if (modifyHeaders != null) {
            modifyHeaders.accept(headers);
        }

        return new ResourceContent(stream(content), headers.build());
    }

    public static ResourceContent partContainerAcl(final String resourceId,
                                                   final String parentId,
                                                   final String rootResourceId,
                                                   final String content) {
        return partAcl(true, resourceId, parentId, rootResourceId, content, null);
    }

    public static ResourceContent partBinaryAcl(final String resourceId,
                                                final String parentId,
                                                final String rootResourceId,
                                                final String content) {
        return partAcl(false, resourceId, parentId, rootResourceId, content, null);
    }

    public static ResourceContent partAcl(final boolean describesRdf,
                                          final String resourceId,
                                          final String parentId,
                                          final String rootResourceId,
                                          final String content,
                                          final Consumer<ResourceHeaders.Builder> modifyHeaders) {
        return atomicAcl(describesRdf, resourceId, parentId, content, headers -> {
            headers.withArchivalGroupId(rootResourceId);

            if (content != null) {
                headers.withContentPath(PersistencePaths.aclResource(describesRdf, rootResourceId, resourceId)
                        .getContentFilePath());
            }

            if (modifyHeaders != null) {
                modifyHeaders.accept(headers);
            }
        });
    }

    public static ResourceContent atomicDesc(final String resourceId,
                                             final String parentId,
                                             final String content) {
        return atomicDesc(resourceId, parentId, content, null);
    }

    public static ResourceContent atomicDesc(final String resourceId,
                                             final String parentId,
                                             final String content,
                                             final Consumer<ResourceHeaders.Builder> modifyHeaders) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.withObjectRoot(false);
        headers.withArchivalGroup(false);
        headers.withInteractionModel(InteractionModel.NON_RDF_DESCRIPTION.getUri());
        headers.withMimeType(RDF_MIME);

        if (content != null) {
            headers.withContentPath(PersistencePaths.rdfResource(parentId, resourceId).getContentFilePath());
        }

        if (modifyHeaders != null) {
            modifyHeaders.accept(headers);
        }

        return new ResourceContent(stream(content), headers.build());
    }

    public static ResourceContent partDesc(final String resourceId,
                                           final String parentId,
                                           final String rootResourceId,
                                           final String content) {
        return partDesc(resourceId, parentId, rootResourceId, content, null);
    }

    public static ResourceContent partDesc(final String resourceId,
                                           final String parentId,
                                           final String rootResourceId,
                                           final String content,
                                           final Consumer<ResourceHeaders.Builder> modifyHeaders) {
        return atomicDesc(resourceId, parentId, content, headers -> {
            headers.withArchivalGroupId(rootResourceId);

            if (content != null) {
                headers.withContentPath(PersistencePaths.rdfResource(rootResourceId, resourceId).getContentFilePath());
            }

            if (modifyHeaders != null) {
                modifyHeaders.accept(headers);
            }
        });
    }

    private static ResourceHeaders.Builder defaultHeaders(final String resourceId,
                                                   final String parentId,
                                                   final String content) {
        final var headers = ResourceHeaders.builder();
        headers.withId(resourceId);
        headers.withParent(parentId);
        headers.withCreatedBy(DEFAULT_USER);
        headers.withCreatedDate(Instant.now());
        headers.withLastModifiedBy(DEFAULT_USER);
        final Instant now = Instant.now();
        headers.withLastModifiedDate(now);
        headers.withStateToken(getStateToken(now));
        if (content != null) {
            headers.withContentSize((long) content.length());
            headers.addDigest(URI.create("urn:sha-512:" + DigestUtils.sha512Hex(content)));
        }
        return headers;
    }

    public static String getStateToken(final Instant timestamp) {
        return DigestUtils.md5Hex(String.valueOf(timestamp.toEpochMilli())).toUpperCase();
    }

    private static InputStream stream(final String value) {
        if (value == null) {
            return null;
        }
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }

}
