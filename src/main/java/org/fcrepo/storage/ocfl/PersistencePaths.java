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

import org.apache.commons.lang3.StringUtils;

/**
 * This class maps Fedora resources to locations on disk. It is based on this wiki:
 * https://wiki.lyrasis.org/display/FF/Design+-+Fedora+OCFL+Object+Structure
 *
 * @author pwinckles
 * @since 6.0.0
 */
public final class PersistencePaths {

    private static final String FCR_METADATA = "fcr:metadata";
    private static final String FCR_ACL = "fcr:acl";

    private static final String HEADER_DIR = ".fcrepo/";
    private static final String ROOT_PREFIX = "fcr-root";
    private static final String CONTAINER_PREFIX = "fcr-container";
    private static final String ACL_SUFFIX = "~fcr-acl";
    private static final String DESCRIPTION_SUFFIX = "~fcr-desc";
    private static final String RDF_EXTENSION = ".nt";
    private static final String JSON_EXTENSION = ".json";

    public static final String ROOT_HEADER_PATH = HEADER_DIR + ROOT_PREFIX + JSON_EXTENSION;

    private final String contentFilePath;
    private final String headerFilePath;

    private PersistencePaths(final String contentFilePath, final String headerFilePath) {
        this.contentFilePath = contentFilePath;
        this.headerFilePath = headerFilePath;
    }

    public String getContentFilePath() {
        return contentFilePath;
    }

    public String getHeaderFilePath() {
        return headerFilePath;
    }

    /**
     * Returns the path to the resourceId's header file. The rootResourceId is the ide of the resource that's at the
     * root of the OCFL object. In the case of atomic resources the rootId and resourceId are one and the same. They
     * are only different for archival parts.
     *
     * @param rootResourceId the id of the resource at the root of the OCFL object
     * @param resourceId the id of the resource to get the header path for
     * @return path to header file
     */
    public static String headerPath(final String rootResourceId, final String resourceId) {
        final var info = analyze(rootResourceId, resourceId);
        return resolveHeaderPath(info);
    }

    /**
     * Returns the paths for a non-RDF resource. The rootResourceId is the id of the resource that's at the root of
     * the OCFL object. In the case of atomic resources the rootResourceId and resourceId are one and the same.
     * They are only different for archival parts.
     *
     * @param rootResourceId the id of the resource at the root of the OCFL object
     * @param resourceId the id of the non-rdf resource to get the paths for
     * @return paths
     */
    public static PersistencePaths nonRdfResource(final String rootResourceId, final String resourceId) {
        final var info = analyze(rootResourceId, resourceId);
        final var headerPath = resolveHeaderPath(info);
        final var contentPath = resolveContentPath(false, info);
        return new PersistencePaths(contentPath, headerPath);
    }

    /**
     * Returns the paths for RDF resources. It should NOT be used for ACL resources. The resourceRootId is the id
     * of the resource that's at the root of the OCFL object. In the case of atomic resources the rootResourceId and
     * resourceId are one and the same. They are only different for archival parts.
     *
     * @param rootResourceId the id of the resource at the root of the OCFL object
     * @param resourceId the id of the rdf resource to get the paths for
     * @return paths
     */
    public static PersistencePaths rdfResource(final String rootResourceId, final String resourceId) {
        final var info = analyze(rootResourceId, resourceId);
        if (info.isAcl) {
            throw new IllegalArgumentException("You must use aclContentPath() for ACL resources.");
        }
        final var headerPath = resolveHeaderPath(info);
        final var contentPath = resolveContentPath(!info.isDescription, info);
        return new PersistencePaths(contentPath, headerPath);
    }

    /**
     * Returns the paths for ACL resources. The rootResourceId is the id of the resource that's at the root of the
     * OCFL object. In the case of atomic resources the rootResourceId and resourceId are one and the same. They
     * are only different for archival parts.
     *
     * @param describesRdfResource indicates if the acl is associated to a rdf resource
     * @param rootResourceId the id of the resource at the root of the OCFL object
     * @param resourceId the id of the acl resource to get the paths for
     * @return paths
     */
    public static PersistencePaths aclResource(final boolean describesRdfResource,
                                               final String rootResourceId, final String resourceId) {
        final var info = analyze(rootResourceId, resourceId);
        if (!info.isAcl) {
            throw new IllegalArgumentException("This function should only be called for ACL resources.");
        }
        final var headerPath = resolveHeaderPath(info);
        final var contentPath = resolveContentPath(describesRdfResource, info);
        return new PersistencePaths(contentPath, headerPath);
    }

    private static String resolveContentPath(final boolean isContainer, final IdInfo info) {
        if (info.isRoot) {
            if (isContainer) {
                if (info.isAcl) {
                    return CONTAINER_PREFIX + ACL_SUFFIX + RDF_EXTENSION;
                } else {
                    return CONTAINER_PREFIX + RDF_EXTENSION;
                }
            }
        }

        final var pathBuilder = new StringBuilder(info.relativeId);

        if (info.isDescription) {
            pathBuilder.append(DESCRIPTION_SUFFIX).append(RDF_EXTENSION);
        } else if (isContainer) {
            if (info.isAcl) {
                pathBuilder.append("/").append(CONTAINER_PREFIX).append(ACL_SUFFIX).append(RDF_EXTENSION);
            } else {
                pathBuilder.append("/").append(CONTAINER_PREFIX).append(RDF_EXTENSION);
            }
        } else if (info.isAcl) {
            pathBuilder.append(ACL_SUFFIX).append(RDF_EXTENSION);
        }

        return pathBuilder.toString();
    }

    private static String resolveHeaderPath(final IdInfo info) {
        String path;

        if (info.isRoot) {
            path = ROOT_PREFIX;
        } else {
            path = info.relativeId;
        }

        if (info.isAcl) {
            path += ACL_SUFFIX;
        } else if (info.isDescription) {
            path += DESCRIPTION_SUFFIX;
        }

        return headerPath(path);
    }

    private static String headerPath(final String path) {
        return HEADER_DIR + path + JSON_EXTENSION;
    }

    private static IdInfo analyze(final String rootResourceId, final String resourceId) {
        if (rootResourceId.equals(resourceId)) {
            return IdInfo.root(resourceId);
        }

        if (!resourceId.startsWith(rootResourceId)) {
            throw new IllegalArgumentException(String.format("The resources %s and %s are unrelated",
                    resourceId, rootResourceId));
        }

        final var relative = resourceId.substring(rootResourceId.length() + 1);

        if (relative.equals(FCR_ACL)) {
            return IdInfo.rootAcl(StringUtils.substringBeforeLast(resourceId, "/"));
        } else if (relative.equals(FCR_METADATA)) {
            return IdInfo.rootDescription(StringUtils.substringBeforeLast(resourceId, "/"));
        }

        final var info = IdInfo.regular(resourceId, relative);

        if (relative.endsWith(FCR_ACL)) {
            info.isAcl = true;
            info.relativeId = StringUtils.substringBeforeLast(relative, "/");
        } else if (relative.endsWith(FCR_METADATA)) {
            info.isDescription = true;
            info.relativeId = StringUtils.substringBeforeLast(relative, "/");
        }

        return info;
    }

    private static class IdInfo {

        String resourceId;
        String relativeId;
        boolean isRoot;
        boolean isAcl;
        boolean isDescription;

        static IdInfo root(final String resourceId) {
            final var lastPart =  resourceId.substring(resourceId.lastIndexOf('/') + 1);
            final var info = new IdInfo(resourceId, lastPart);
            info.isRoot = true;
            return info;
        }

        static IdInfo rootAcl(final String resourceId) {
            final var info = root(resourceId);
            info.isAcl = true;
            return info;
        }

        static IdInfo rootDescription(final String resourceId) {
            final var info = root(resourceId);
            info.isDescription = true;
            return info;
        }

        static IdInfo regular(final String resourceId, final String relativeId) {
            return new IdInfo(resourceId, relativeId);
        }

        IdInfo(final String resourceId, final String relativeId) {
            this.resourceId = resourceId;
            this.relativeId = relativeId;
            this.isRoot = false;
            this.isAcl = false;
            this.isDescription = false;
        }
    }

}
