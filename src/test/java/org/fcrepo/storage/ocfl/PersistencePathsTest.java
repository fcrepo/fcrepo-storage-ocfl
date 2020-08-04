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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author pwinckles
 */
public class PersistencePathsTest {

    private static final String FCREPO_DIR = ".fcrepo/";
    private static final String ROOT_PREFIX = "fcr-root";
    private static final String CONTAINER_PREFIX = "fcr-container";
    private static final String ACL_SUFFIX = "~fcr-acl";
    private static final String DESC_SUFFIX = "~fcr-desc";
    private static final String JSON = ".json";
    private static final String NT = ".nt";

    @Test
    public void createRootResourceHeaderPath() {
        final var id = resourceId("binary");
        assertEquals(FCREPO_DIR + ROOT_PREFIX + JSON, PersistencePaths.headerPath(id, id));
    }

    @Test
    public void createAgResourceHeaderPath() {
        final var rootId = resourceId("ag");
        final var id = resourceId("ag/sub/path");
        assertEquals(FCREPO_DIR + "sub/path.json", PersistencePaths.headerPath(rootId, id));
    }

    @Test
    public void createAgAclResourceHeaderPath() {
        final var rootId = resourceId("ag");
        final var id = resourceId(acl("ag/sub/path"));
        assertEquals(FCREPO_DIR + "sub/path" + ACL_SUFFIX + JSON, PersistencePaths.headerPath(rootId, id));
    }

    @Test
    public void createAgDescriptionResourceHeaderPath() {
        final var rootId = resourceId("ag");
        final var id = resourceId(desc("ag/sub/path"));
        assertEquals(FCREPO_DIR + "sub/path" + DESC_SUFFIX + JSON, PersistencePaths.headerPath(rootId, id));
    }

    @Test
    public void createRootAclResourceHeaderPath() {
        final var rootId = resourceId("ag");
        final var id = resourceId(acl("ag"));
        assertEquals(FCREPO_DIR + ROOT_PREFIX + ACL_SUFFIX + JSON, PersistencePaths.headerPath(rootId, id));
    }

    @Test
    public void createRootDescriptionResourceHeaderPath() {
        final var rootId = resourceId("ag");
        final var id = resourceId(desc("ag"));
        assertEquals(FCREPO_DIR + ROOT_PREFIX + DESC_SUFFIX + JSON, PersistencePaths.headerPath(rootId, id));
    }

    @Test
    public void createContentPathForAtomicBinary() {
        final var rootId = resourceId("binary");
        final var id = resourceId("binary");
        final var paths = PersistencePaths.nonRdfResource(rootId, id);
        assertEquals("binary", paths.getContentFilePath());
        assertEquals(FCREPO_DIR + ROOT_PREFIX + JSON, paths.getHeaderFilePath());
    }

    @Test
    public void createContentPathForAgBinary() {
        final var rootId = resourceId("ag");
        final var id = resourceId("ag/sub/binary");
        final var paths = PersistencePaths.nonRdfResource(rootId, id);
        assertEquals("sub/binary", paths.getContentFilePath());
        assertEquals(FCREPO_DIR + "sub/binary" + JSON, paths.getHeaderFilePath());
    }

    @Test
    public void createContentPathForAtomicContainer() {
        final var rootId = resourceId("object");
        final var id = resourceId("object");
        final var paths = PersistencePaths.rdfResource(rootId, id);
        assertEquals(CONTAINER_PREFIX + NT, paths.getContentFilePath());
        assertEquals(FCREPO_DIR + ROOT_PREFIX + JSON, paths.getHeaderFilePath());
    }

    @Test
    public void createContentPathForAgContainer() {
        final var rootId = resourceId("ag");
        final var id = resourceId("ag/foo/bar");
        final var paths = PersistencePaths.rdfResource(rootId, id);
        assertEquals("foo/bar/" + CONTAINER_PREFIX + NT, paths.getContentFilePath());
        assertEquals(FCREPO_DIR + "foo/bar" + JSON, paths.getHeaderFilePath());
    }

    @Test
    public void createContentPathForAtomicBinaryDesc() {
        final var rootId = resourceId("binary");
        final var id = resourceId(desc("binary"));
        final var paths = PersistencePaths.rdfResource(rootId, id);
        assertEquals("binary" + DESC_SUFFIX + NT, paths.getContentFilePath());
        assertEquals(FCREPO_DIR + ROOT_PREFIX + DESC_SUFFIX + JSON, paths.getHeaderFilePath());
    }

    @Test
    public void createContentPathForAgBinaryDesc() {
        final var rootId = resourceId("ag");
        final var id = resourceId(desc("ag/sub/binary"));
        final var paths = PersistencePaths.rdfResource(rootId, id);
        assertEquals("sub/binary" + DESC_SUFFIX + NT, paths.getContentFilePath());
        assertEquals(FCREPO_DIR + "sub/binary" + DESC_SUFFIX + JSON, paths.getHeaderFilePath());
    }

    @Test
    public void createContentPathForAtomicBinaryAcl() {
        final var rootId = resourceId("binary");
        final var id = resourceId(acl("binary"));
        final var paths = PersistencePaths.aclResource(false, rootId, id);
        assertEquals("binary" + ACL_SUFFIX + NT, paths.getContentFilePath());
        assertEquals(FCREPO_DIR + ROOT_PREFIX + ACL_SUFFIX + JSON, paths.getHeaderFilePath());
    }

    @Test
    public void createContentPathForAgBinaryAcl() {
        final var rootId = resourceId("ag");
        final var id = resourceId(acl("ag/sub/binary"));
        final var paths = PersistencePaths.aclResource(false, rootId, id);
        assertEquals("sub/binary" + ACL_SUFFIX + NT, paths.getContentFilePath());
        assertEquals(FCREPO_DIR + "sub/binary" + ACL_SUFFIX + JSON, paths.getHeaderFilePath());
    }

    @Test
    public void createContentPathForAtomicContainerAcl() {
        final var rootId = resourceId("object");
        final var id = resourceId(acl("object"));
        final var paths = PersistencePaths.aclResource(true, rootId, id);
        assertEquals(CONTAINER_PREFIX + ACL_SUFFIX + NT, paths.getContentFilePath());
        assertEquals(FCREPO_DIR + ROOT_PREFIX + ACL_SUFFIX + JSON, paths.getHeaderFilePath());
    }

    @Test
    public void createContentPathForAgContainerAcl() {
        final var rootId = resourceId("ag");
        final var id = resourceId(acl("ag/foo/bar"));
        final var paths = PersistencePaths.aclResource(true, rootId, id);
        assertEquals("foo/bar/" + CONTAINER_PREFIX + ACL_SUFFIX + NT, paths.getContentFilePath());
        assertEquals(FCREPO_DIR + "foo/bar" + ACL_SUFFIX + JSON, paths.getHeaderFilePath());
    }

    private String resourceId(final String id) {
        return "info:fedora/" + id;
    }

    private String acl(final String id) {
        return id + "/fcr:acl";
    }

    private String desc(final String id) {
        return id + "/fcr:metadata";
    }

}
