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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleConfig;
import edu.wisc.library.ocfl.core.path.mapper.LogicalPathMappers;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author pwinckles
 */
public class DefaultOcflObjectSessionTest {

    @Rule
    public TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    private Path ocflRoot;
    private Path sessionStaging;

    private MutableOcflRepository ocflRepo;
    private OcflObjectSessionFactory sessionFactory;

    private static final String ROOT = "info:fedora";
    private static final String DEFAULT_AG_ID = "info:fedora/foo";
    private static final String DEFAULT_AG_BINARY_ID = "info:fedora/foo/bar";
    private static final String DEFAULT_MESSAGE = "F6 migration";
    private static final String DEFAULT_USER = "fedoraAdmin";
    private static final String DEFAULT_ADDRESS = "info:fedora/fedoraAdmin";

    @Before
    public void setup() throws IOException {
        ocflRoot = temp.newFolder("ocfl").toPath();
        sessionStaging = temp.newFolder("staging").toPath();
        final var ocflTemp = temp.newFolder("ocfl-temp").toPath();

        final var logicalPathMapper = SystemUtils.IS_OS_WINDOWS ?
                LogicalPathMappers.percentEncodingWindowsMapper() : LogicalPathMappers.percentEncodingLinuxMapper();

        ocflRepo = new OcflRepositoryBuilder()
                .layoutConfig(new HashedTruncatedNTupleConfig())
                .logicalPathMapper(logicalPathMapper)
                .storage(FileSystemOcflStorage.builder().repositoryRoot(ocflRoot).build())
                .workDir(ocflTemp)
                .buildMutable();

        final var objectMapper = new ObjectMapper()
                .configure(WRITE_DATES_AS_TIMESTAMPS, false)
                .registerModule(new JavaTimeModule())
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);

        sessionFactory = new DefaultOcflObjectSessionFactory(ocflRepo, sessionStaging, objectMapper,
                CommitType.NEW_VERSION, DEFAULT_MESSAGE, DEFAULT_USER, DEFAULT_ADDRESS);
    }

    @Test
    public void writeNewNonRdfResource() {
        final var resourceId = "info:fedora/foo/bar";
        final var session = sessionFactory.newSession(resourceId);

        final var contentStr = "Test";
        final var content = atomicBinary(resourceId, "info:fedora/foo", contentStr);

        write(session, content);

        final var stagedContent = session.readContent(resourceId);

        assertResourceContent(contentStr, content, stagedContent);

        session.commit();

        final var committedContent = session.readContent(resourceId);

        assertResourceContent(contentStr, content, committedContent);
    }

    @Test
    public void writeUpdateNonRdfResource() {
        final var resourceId = "info:fedora/foo/bar";

        final var session1 = sessionFactory.newSession(resourceId);
        final var content1 = atomicBinary(resourceId, "info:fedora/foo", "Test");

        write(session1, content1);
        session1.commit();

        final var session2 = sessionFactory.newSession(resourceId);
        final var contentStr2 = "Updated!";
        final var content2 = atomicBinary(resourceId, "info:fedora/foo", contentStr2);

        write(session2, content2);
        session2.commit();

        final var committedContent = session2.readContent(resourceId);

        assertResourceContent(contentStr2, content2, committedContent);

        assertEquals(2, ocflRepo.describeObject(resourceId).getVersionMap().size());
    }

    @Test
    public void writeNewAtomicRdfResource() {
        final var resourceId = "info:fedora/foo/bar";
        final var session = sessionFactory.newSession(resourceId);

        final var contentStr = "Test";
        final var content = atomicContainer(resourceId, "info:fedora/foo", contentStr);

        write(session, content);

        final var stagedContent = session.readContent(resourceId);

        assertResourceContent(contentStr, content, stagedContent);

        session.commit();

        final var committedContent = session.readContent(resourceId);

        assertResourceContent(contentStr, content, committedContent);
    }

    @Test
    public void writeNewAg() {
        final var agId = "info:fedora/foo";
        final var containerId = "info:fedora/foo/bar";
        final var binaryId = "info:fedora/foo/bar/baz";

        final var session = sessionFactory.newSession(agId);

        final var agContent = ag(agId, ROOT, "foo");
        final var containerContent = container(containerId, agId, "bar");
        final var binaryContent = binary(binaryId, containerId, "baz");

        write(session, agContent);
        write(session, containerContent);
        write(session, binaryContent);

        final var stagedContent = session.readContent(binaryId);

        assertResourceContent("baz", binaryContent, stagedContent);

        session.commit();

        final var committedAg = session.readContent(agId);
        final var committedContainer = session.readContent(containerId);
        final var committedBinary = session.readContent(binaryId);

        assertResourceContent("foo", agContent, committedAg);
        assertResourceContent("bar", containerContent, committedContainer);
        assertResourceContent("baz", binaryContent, committedBinary);
    }

    @Test
    public void writeNewBinaryAcl() {
        final var resourceId = "info:fedora/foo/bar";
        final var aclId = "info:fedora/foo/bar/fcr:acl";

        final var session = sessionFactory.newSession(resourceId);

        final var content = atomicBinary(resourceId, "info:fedora/foo", "blah");
        final var acl = acl(aclId, resourceId, "acl");

        write(session, content);
        write(session, acl);

        session.commit();

        final var committedAcl = session.readContent(aclId);

        assertResourceContent("acl", acl, committedAcl);
    }

    @Test
    public void writeNewContainerAcl() {
        final var resourceId = "info:fedora/foo/bar";
        final var aclId = "info:fedora/foo/bar/fcr:acl";

        final var session = sessionFactory.newSession(resourceId);

        final var content = atomicContainer(resourceId, "info:fedora/foo", "blah");
        final var acl = acl(aclId, resourceId, "acl");

        write(session, content);
        write(session, acl);

        session.commit();

        final var committedAcl = session.readContent(aclId);

        assertResourceContent("acl", acl, committedAcl);
    }

    @Test
    public void writeNewBinaryDesc() {
        final var resourceId = "info:fedora/foo/bar";
        final var descId = "info:fedora/foo/bar/fcr:metadata";

        final var session = sessionFactory.newSession(resourceId);

        final var content = atomicBinary(resourceId, "info:fedora/foo", "blah");
        final var desc = desc(descId, resourceId, "desc");

        write(session, content);
        write(session, desc);

        session.commit();

        final var committedDesc = session.readContent(descId);

        assertResourceContent("desc", desc, committedDesc);
    }

    @Test(expected = NotFoundException.class)
    public void throwExceptionWhenObjectDoesNotExist() {
        final var resourceId = "info:fedora/foo/bar";
        final var session = sessionFactory.newSession(resourceId);
        session.readContent(resourceId);
    }

    @Test(expected = NotFoundException.class)
    public void throwExceptionWhenObjectExistButResourceDoesNot() {
        final var resourceId = "info:fedora/foo/bar";
        final var session = sessionFactory.newSession(resourceId);

        write(session, atomicBinary(resourceId, "info:fedora/foo", "blah"));
        session.commit();

        session.readContent(resourceId + "/baz");
    }

    @Test
    public void deleteFileThatHasBeenCommitted() {
        close(defaultAg());
        final var agBinary = defaultAgBinary();
        close(agBinary);

        final var session1 = sessionFactory.newSession(DEFAULT_AG_ID);

        final var existing = session1.readContent(DEFAULT_AG_BINARY_ID);

        assertEquals(agBinary.getHeaders(), existing.getHeaders());
        assertTrue(existing.getContentStream().isPresent());
        close(existing);

        existing.getHeaders().setDeleted(true);

        session1.deleteContentFile(existing.getHeaders());

        final var stagedDelete = session1.readContent(DEFAULT_AG_BINARY_ID);

        assertEquals(existing.getHeaders(), stagedDelete.getHeaders());
        assertFalse(stagedDelete.getContentStream().isPresent());

        session1.commit();

        final var deleted = session1.readContent(DEFAULT_AG_BINARY_ID);

        assertEquals(existing.getHeaders(), deleted.getHeaders());
        assertFalse(deleted.getContentStream().isPresent());

        final var session2 = sessionFactory.newSession(DEFAULT_AG_ID);

        session2.deleteResource(DEFAULT_AG_BINARY_ID);

        expectResourceNotFound(DEFAULT_AG_BINARY_ID, session2);

        session2.commit();

        expectResourceNotFound(DEFAULT_AG_BINARY_ID, session2);
    }

    @Test
    public void deleteFileThatHasNotBeenCommitted() {
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        final var ag = ag(DEFAULT_AG_ID, ROOT, "ag");
        final var binary = binary(DEFAULT_AG_BINARY_ID, DEFAULT_AG_ID, "binary");

        write(session, ag);
        write(session, binary);

        session.deleteContentFile(binary.getHeaders());

        expectResourceNotFound(DEFAULT_AG_BINARY_ID, session);

        session.commit();

        expectResourceNotFound(DEFAULT_AG_BINARY_ID, session);
        assertResourceContent("ag", ag, session.readContent(DEFAULT_AG_ID));
    }

    @Test
    public void deleteFileAndThenReAddBeforeCommitting() {
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        final var ag = ag(DEFAULT_AG_ID, ROOT, "ag");
        final var binary = binary(DEFAULT_AG_BINARY_ID, DEFAULT_AG_ID, "binary");
        final var binary2 = binary(DEFAULT_AG_BINARY_ID, DEFAULT_AG_ID, "binary2");

        write(session, ag);
        write(session, binary);

        session.deleteContentFile(binary.getHeaders());
        expectResourceNotFound(DEFAULT_AG_BINARY_ID, session);

        write(session, binary2);

        session.commit();

        assertResourceContent("ag", ag, session.readContent(DEFAULT_AG_ID));
        assertResourceContent("binary2", binary2, session.readContent(DEFAULT_AG_BINARY_ID));
    }

    @Test
    public void deleteObjectWhenAlreadyCommitted() {
        close(defaultAg());
        close(defaultAgBinary());

        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        session.deleteResource(DEFAULT_AG_ID);
        session.commit();

        expectResourceNotFound(DEFAULT_AG_ID, session);
        expectResourceNotFound(DEFAULT_AG_BINARY_ID, session);

        assertFalse(ocflRepo.containsObject(DEFAULT_AG_ID));
    }

    @Test
    public void deleteObjectWhenNotCommitted() {
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        final var ag = ag(DEFAULT_AG_ID, ROOT, "ag");
        final var binary = binary(DEFAULT_AG_BINARY_ID, DEFAULT_AG_ID, "binary");

        write(session, ag);
        write(session, binary);

        session.deleteResource(DEFAULT_AG_ID);

        session.commit();

        expectResourceNotFound(DEFAULT_AG_ID, session);
        expectResourceNotFound(DEFAULT_AG_BINARY_ID, session);

        assertFalse(ocflRepo.containsObject(DEFAULT_AG_ID));
    }

    @Test
    public void addedFilesToDeletedObject() {
        close(defaultAg());
        close(defaultAgBinary());

        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        session.deleteResource(DEFAULT_AG_ID);

        final var ag2 = ag(DEFAULT_AG_ID, ROOT, "ag2");

        write(session, ag2);

        session.commit();

        expectResourceNotFound(DEFAULT_AG_BINARY_ID, session);
        assertResourceContent("ag2", ag2, session.readContent(DEFAULT_AG_ID));
    }

    @Test
    public void readHeadersForResourceMarkedAsDeleted() {
        final var ag = defaultAg();
        close(ag);

        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        ag.getHeaders().setDeleted(true);

        session.deleteContentFile(ag.getHeaders());
        session.commit();

        final var deletedContent = session.readContent(DEFAULT_AG_ID);

        assertEquals(ag.getHeaders(), deletedContent.getHeaders());
        assertFalse(deletedContent.getContentStream().isPresent());
    }

    @Test
    public void setOcflVersionInfo() {
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        final var ag = ag(DEFAULT_AG_ID, ROOT, "ag");

        write(session, ag);

        final var created = OffsetDateTime.now().minusWeeks(2);
        final var message = "Special message!";
        final var author = "John Doe";
        final var address = "jdoe@example.com";

        session.versionAuthor(author, address);
        session.versionMessage(message);
        session.versionCreationTimestamp(created);

        session.commit();

        final var desc = ocflRepo.describeVersion(ObjectVersionId.head(DEFAULT_AG_ID));

        assertEquals(created, desc.getCreated());
        assertEquals(message, desc.getVersionInfo().getMessage());
        assertEquals(author, desc.getVersionInfo().getUser().getName());
        assertEquals(address, desc.getVersionInfo().getUser().getAddress());
    }

    @Test
    public void abortStagedChanges() {
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        final var ag = ag(DEFAULT_AG_ID, ROOT, "ag");
        write(session, ag);

        session.abort();

        assertFalse(ocflRepo.containsObject(DEFAULT_AG_ID));
    }

    @Test
    public void failWriteAfterSessionClosed() {
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        final var ag = ag(DEFAULT_AG_ID, ROOT, "ag");
        write(session, ag);

        session.commit();

        try {
            write(session, ag);
            fail("Should have thrown an exception");
        } catch (IllegalStateException e) {
            // expected exception
        }
    }

    @Test
    public void failCommitAfterSessionClosed() {
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        final var ag = ag(DEFAULT_AG_ID, ROOT, "ag");
        write(session, ag);

        session.commit();

        try {
            session.commit();
            fail("Should have thrown an exception");
        } catch (IllegalStateException e) {
            // expected exception
        }
    }

    @Test
    public void ensurePathsAreSafeForWindows() {
        final var resourceId = "info:fedora/foo:bar";
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        final var content = atomicBinary(resourceId, ROOT, "stuff");

        write(session, content);

        final var stagedContent = session.readContent(resourceId);
        assertResourceContent("stuff", content, stagedContent);

        session.commit();

        final var committedContent = session.readContent(resourceId);
        assertResourceContent("stuff", content, committedContent);
    }

    @Test
    public void doNothingWhenNoStagedChangesAndObjectDoesNotExist() {
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        session.commit();
        assertFalse(ocflRepo.containsObject(DEFAULT_AG_ID));
    }

    @Test
    public void doNothingWhenNoStagedChangesAndObjectExist() {
        close(defaultAg());
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        session.commit();
        assertEquals(1, ocflRepo.describeObject(DEFAULT_AG_ID).getVersionMap().size());
    }

    @Test
    public void restoreAtomicRdfResource() {
        final var resourceId = "info:fedora/foo/bar";
        final var session = sessionFactory.newSession(resourceId);

        final var contentStr = "Test";
        final var content = atomicContainer(resourceId, "info:fedora/foo", contentStr);

        write(session, content);

        final var stagedContent = session.readContent(resourceId);

        assertResourceContent(contentStr, content, stagedContent);

        session.commit();

        final var committedContent = session.readContent(resourceId);

        assertResourceContent(contentStr, content, committedContent);

        // Start a second session to delete and then recreate the resource
        final var session2 = sessionFactory.newSession(resourceId);
        session2.deleteResource(resourceId);

        final var contentStr2 = "Test more";
        final var content2 = atomicContainer(resourceId, "info:fedora/foo", contentStr2);

        write(session2, content2);

        final var stagedContent2 = session2.readContent(resourceId);
        assertResourceContent(contentStr2, content2, stagedContent2);

        session2.commit();

        final var committedContent2 = session2.readContent(resourceId);

        assertResourceContent(contentStr2, content2, committedContent2);
    }

    @Test
    public void writeFileWithNullContent() {
        final var resourceId = "info:fedora/foo";
        final var session = sessionFactory.newSession(resourceId);

        final var content = atomicBinary(resourceId, ROOT, null);

        write(session, content);

        final var stagedContent = session.readContent(resourceId);
        assertResourceContent(null, content, stagedContent);

        session.commit();

        final var committedContent = session.readContent(resourceId);
        assertResourceContent(null, content, committedContent);
        assertNull(content.getHeaders().getContentPath());
    }

    @Test
    public void listAgVersions() {
        final var binary2Id = DEFAULT_AG_ID + "/baz";

        final var session1 = sessionFactory.newSession(DEFAULT_AG_ID);

        write(session1, ag(DEFAULT_AG_ID, ROOT, "ag"));
        write(session1, binary(DEFAULT_AG_BINARY_ID, DEFAULT_AG_ID, "binary"));
        session1.commit();

        final var session2 = sessionFactory.newSession(DEFAULT_AG_ID);

        write(session2, binary(binary2Id, DEFAULT_AG_ID, "binary2"));
        session2.commit();

        final var session3 = sessionFactory.newSession(DEFAULT_AG_ID);

        write(session3, binary(DEFAULT_AG_BINARY_ID, DEFAULT_AG_ID, "updated"));
        session3.commit();

        assertThat(session3.listVersions(DEFAULT_AG_ID).stream()
                .map(OcflVersionInfo::getVersionNumber)
                .collect(Collectors.toList()), contains("v1"));

        assertThat(session3.listVersions(DEFAULT_AG_BINARY_ID).stream()
                .map(OcflVersionInfo::getVersionNumber)
                .collect(Collectors.toList()), contains("v1", "v3"));

        assertThat(session3.listVersions(binary2Id).stream()
                .map(OcflVersionInfo::getVersionNumber)
                .collect(Collectors.toList()), contains("v2"));
    }

    @Test(expected = NotFoundException.class)
    public void throwExceptionWhenListingVersionsOnObjectThatDoesNotExist() {
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        session.listVersions(DEFAULT_AG_ID);
    }

    @Test(expected = NotFoundException.class)
    public void throwExceptionWhenListingVersionsOnResourceThatDoesNotExist() {
        close(defaultAg());
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        session.listVersions(DEFAULT_AG_BINARY_ID);
    }

    @Test
    public void returnEmptyVersionsWhenResourceIsStagedButNotInOcfl() {
        close(defaultAg());
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        write(session, binary(DEFAULT_AG_BINARY_ID, DEFAULT_AG_ID, "test"));

        assertEquals(0, session.listVersions(DEFAULT_AG_BINARY_ID).size());
    }

    @Test
    public void returnEmptyVersionsWhenResourceOnlyExistInMutableHead() {
        close(defaultAg());
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        session.commitType(CommitType.UNVERSIONED);

        write(session, binary(DEFAULT_AG_BINARY_ID, DEFAULT_AG_ID, "test"));
        session.commit();

        assertEquals(0, session.listVersions(DEFAULT_AG_BINARY_ID).size());
        assertEquals(1, session.listVersions(DEFAULT_AG_ID).size());
    }

    @Test
    public void readPreviousVersion() {
        final var resourceId = "info:fedora/foo";

        final var first = atomicBinary(resourceId, ROOT, "first");
        final var second = atomicBinary(resourceId, ROOT, "second");
        final var third = atomicBinary(resourceId, ROOT, "third");

        final var session1 = sessionFactory.newSession(DEFAULT_AG_ID);
        write(session1, first);
        session1.commit();

        final var session2 = sessionFactory.newSession(DEFAULT_AG_ID);
        write(session2, second);
        session2.commit();

        final var session3 = sessionFactory.newSession(DEFAULT_AG_ID);
        write(session3, third);
        session3.commit();

        assertResourceContent("first", first, session3.readContent(resourceId, "v1"));
        assertResourceContent("second", second, session3.readContent(resourceId, "v2"));
        assertResourceContent("third", third, session3.readContent(resourceId, "v3"));

        try {
            session3.readContent(resourceId, "v4");
            fail("Expected an exception because the version should not exist");
        } catch (NotFoundException e) {
            // expected exception
        }
    }

    @Test(expected = FixityCheckException.class)
    public void failWhenProvidedDigestDoesNotMatchComputed() throws URISyntaxException {
        final var resourceId = "info:fedora/foo";
        final var content = atomicBinary(resourceId, ROOT, "bar");
        content.getHeaders().setDigests(List.of(
                new URI("urn:sha-512:dc6b68d13b8cf959644b935f1192b02c71aa7a5cf653bd43b4480fa89eec8d4d3f16a" +
                        "2278ec8c3b40ab1fdb233b3173a78fd83590d6f739e0c9e8ff56c282557")));

        final var session = sessionFactory.newSession(resourceId);

        write(session, content);
        session.commit();
    }

    @Test
    public void addOcflDigestWhenNotProvided() throws URISyntaxException {
        final var resourceId = "info:fedora/foo";
        final var content = atomicBinary(resourceId, ROOT, "bar");

        final var sha512Digest = "d82c4eb5261cb9c8aa9855edd67d1bd10482f41529858d925094d173fa662aa" +
                "91ff39bc5b188615273484021dfb16fd8284cf684ccf0fc795be3aa2fc1e6c181";
        final var sha256Digest = "fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9";

        final var digests = new ArrayList<URI>();
        digests.add(new URI("urn:sha-256:" + sha256Digest));

        content.getHeaders().setDigests(digests);

        final var session = sessionFactory.newSession(resourceId);

        write(session, content);
        session.commit();

        final var headers = session.readHeaders(resourceId);

        assertThat(headers.getDigests(), containsInAnyOrder(
                new URI("urn:sha-256:" + sha256Digest),
                new URI("urn:sha-512:" + sha512Digest)));
    }

    @Test
    public void commitToMutableHeadWhenNewObject() {
        final var resourceId = "info:fedora/foo";

        final var content1 = atomicBinary(resourceId, ROOT, "first");
        final var session1 = sessionFactory.newSession(resourceId);
        session1.commitType(CommitType.UNVERSIONED);

        write(session1, content1);
        session1.commit();

        assertResourceContent("first", content1, session1.readContent(resourceId));

        final var content2 = atomicBinary(resourceId, ROOT, "second");
        final var session2 = sessionFactory.newSession(resourceId);
        session2.commitType(CommitType.UNVERSIONED);

        write(session2, content2);
        session2.commit();

        assertResourceContent("second", content2, session2.readContent(resourceId));

        assertEquals(0, session2.listVersions(resourceId).size());
    }

    @Test
    public void commitToMutableHeadWhenHasExistingVersion() {
        final var resourceId = "info:fedora/foo";

        final var content1 = atomicBinary(resourceId, ROOT, "first");
        final var session1 = sessionFactory.newSession(resourceId);

        write(session1, content1);
        session1.commit();

        assertResourceContent("first", content1, session1.readContent(resourceId));

        final var content2 = atomicBinary(resourceId, ROOT, "second");
        final var session2 = sessionFactory.newSession(resourceId);
        session2.commitType(CommitType.UNVERSIONED);

        write(session2, content2);
        session2.commit();

        assertResourceContent("second", content2, session2.readContent(resourceId));

        assertEquals(1, session2.listVersions(resourceId).size());

        assertResourceContent("first", content1, session2.readContent(resourceId, "v1"));
    }

    @Test
    public void commitNewVersionWhenHasStagedChanges() {
        final var resourceId = "info:fedora/foo";

        final var content1 = atomicBinary(resourceId, ROOT, "first");
        final var session1 = sessionFactory.newSession(resourceId);
        session1.commitType(CommitType.UNVERSIONED);

        write(session1, content1);
        session1.commit();

        assertResourceContent("first", content1, session1.readContent(resourceId));

        final var content2 = atomicBinary(resourceId, ROOT, "second");
        final var session2 = sessionFactory.newSession(resourceId);

        write(session2, content2);
        session2.commit();

        assertResourceContent("second", content2, session2.readContent(resourceId));

        assertEquals(1, session2.listVersions(resourceId).size());

        assertResourceContent("second", content2, session2.readContent(resourceId, "v2"));
    }

    @Test
    public void listResourcesWhenOnlyOneWithNothingStaged() {
        final var ag = defaultAg();
        close(ag);

        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        final var resources = session.streamResourceHeaders().collect(Collectors.toList());

        assertThat(resources, containsInAnyOrder(ag.getHeaders()));
    }

    @Test
    public void listResourcesWhenMultipleWithNothingStaged() {
        final var ag = defaultAg();
        final var binary = defaultAgBinary();
        close(ag);
        close(binary);

        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        final var resources = session.streamResourceHeaders().collect(Collectors.toList());

        assertThat(resources, containsInAnyOrder(ag.getHeaders(), binary.getHeaders()));
    }

    @Test
    public void listResourcesWhenMultipleWithStagedChanges() {
        final var ag = defaultAg();
        final var binary = defaultAgBinary();
        close(ag);
        close(binary);

        final var binaryUpdate = binary(DEFAULT_AG_BINARY_ID, DEFAULT_AG_ID, "updated!");

        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        write(session, binaryUpdate);

        final var resources = session.streamResourceHeaders().collect(Collectors.toList());

        assertThat(resources, containsInAnyOrder(ag.getHeaders(), binaryUpdate.getHeaders()));
    }

    @Test
    public void listResourcesWhenMultipleWithStagedDeletes() {
        final var ag = defaultAg();
        final var binary = defaultAgBinary();
        close(ag);
        close(binary);

        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        session.deleteContentFile(binary.getHeaders());

        final var resources = session.streamResourceHeaders().collect(Collectors.toList());
        assertThat(resources, containsInAnyOrder(ag.getHeaders(), binary.getHeaders()));

        session.deleteResource(DEFAULT_AG_BINARY_ID);

        final var resources2 = session.streamResourceHeaders().collect(Collectors.toList());
        assertThat(resources2, containsInAnyOrder(ag.getHeaders()));
    }

    @Test
    public void listResourceWhenNotCreated() {
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        final var resources = session.streamResourceHeaders().collect(Collectors.toList());
        assertEquals(0, resources.size());
    }

    @Test
    public void listResourceWhenPendingDelete() {
        close(defaultAg());
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        session.deleteResource(DEFAULT_AG_ID);
        final var resources = session.streamResourceHeaders().collect(Collectors.toList());
        assertEquals(0, resources.size());
    }

    @Test
    public void containsResourceWhenExistsInOcfl() {
        close(defaultAg());
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        assertTrue(session.containsResource(DEFAULT_AG_ID));
    }

    @Test
    public void containsResourceWhenExistsInStaging() {
        final var resourceId = "info:fedora/foo";

        final var content = atomicBinary(resourceId, ROOT, "first");
        final var session = sessionFactory.newSession(resourceId);

        write(session, content);

        assertTrue(session.containsResource(resourceId));
    }

    @Test
    public void containsResourceWhenContentDeleted() {
        final var resourceId = "info:fedora/foo";

        final var content = atomicBinary(resourceId, ROOT, "first");
        final var session = sessionFactory.newSession(resourceId);

        write(session, content);
        session.commit();

        final var session2 = sessionFactory.newSession(resourceId);

        session2.deleteContentFile(content.getHeaders());

        assertTrue(session2.containsResource(resourceId));
    }

    @Test
    public void notContainsResourceWhenPendingDelete() {
        final var resourceId = "info:fedora/foo";

        final var content = atomicBinary(resourceId, ROOT, "first");
        final var session = sessionFactory.newSession(resourceId);

        write(session, content);
        session.commit();

        final var session2 = sessionFactory.newSession(resourceId);

        session2.deleteResource(resourceId);

        assertFalse(session2.containsResource(resourceId));
    }

    @Test
    public void notContainsResourceWhenObjectNotExists() {
        final var resourceId = "info:fedora/foo";
        final var session = sessionFactory.newSession(resourceId);
        assertFalse(session.containsResource(resourceId));
    }

    @Test
    public void notContainsResourceWhenNotExists() {
        close(defaultAg());
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        assertFalse(session.containsResource(DEFAULT_AG_BINARY_ID));
    }

    @Test
    public void readPreviousVersionWhenHasChangesPending() {
        final var resourceId = "info:fedora/foo";

        final var first = atomicBinary(resourceId, ROOT, "first");
        final var second = atomicBinary(resourceId, ROOT, "second");
        final var third = atomicBinary(resourceId, ROOT, "third");

        final var session1 = sessionFactory.newSession(DEFAULT_AG_ID);
        write(session1, first);
        session1.commit();

        final var session2 = sessionFactory.newSession(DEFAULT_AG_ID);
        write(session2, second);
        session2.commit();

        final var session3 = sessionFactory.newSession(DEFAULT_AG_ID);
        write(session3, third);

        assertResourceContent("first", first, session3.readContent(resourceId, "v1"));
        assertResourceContent("second", second, session3.readContent(resourceId, "v2"));
        assertResourceContent("third", third, session3.readContent(resourceId));
    }

    @Test
    public void touchLastModifiedDateOnExistingResource() {
        close(defaultAg());

        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        final var original = session.readHeaders(DEFAULT_AG_ID).getLastModifiedDate();

        session.touchResource(DEFAULT_AG_ID);
        session.commit();

        final var touched = session.readHeaders(DEFAULT_AG_ID).getLastModifiedDate();

        assertNotEquals(original, touched);
        assertEquals(2, session.listVersions(DEFAULT_AG_ID).size());
    }

    @Test
    public void touchLastModifiedDateOnStagedResource() {
        final var resourceId = "info:fedora/foo";
        final var content = atomicBinary(resourceId, ROOT, "first");

        final var session = sessionFactory.newSession(resourceId);

        write(session, content);

        final var original = session.readHeaders(resourceId).getLastModifiedDate();

        session.touchResource(resourceId);
        session.commit();

        final var touched = session.readHeaders(resourceId).getLastModifiedDate();

        assertNotEquals(original, touched);
        assertEquals(1, session.listVersions(resourceId).size());
    }

    @Test(expected = NotFoundException.class)
    public void failTouchWhenResourceDoesNotExist() {
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        session.touchResource(DEFAULT_AG_ID);
    }

    private void assertResourceContent(final String content,
                                       final ResourceContent expected,
                                       final ResourceContent actual) {
        if (content == null) {
            assertTrue("content should have been null", actual.getContentStream().isEmpty());
        } else {
            assertEquals(content, toString(actual.getContentStream()));
        }
        assertEquals(expected.getHeaders(), actual.getHeaders());
    }

    private void expectResourceNotFound(final String resourceId, final OcflObjectSession session) {
        try {
            session.readContent(resourceId);
            fail(String.format("Expected resource %s to not exist", resourceId));
        } catch (NotFoundException e) {
            // Expected exception
        }
    }

    private ResourceContent defaultAg() {
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        write(session, ag(DEFAULT_AG_ID, ROOT, "ag"));
        session.commit();
        return session.readContent(DEFAULT_AG_ID);
    }

    private ResourceContent defaultAgBinary() {
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        write(session, binary(DEFAULT_AG_BINARY_ID, DEFAULT_AG_ID, "bar"));
        session.commit();
        return session.readContent(DEFAULT_AG_BINARY_ID);
    }

    private ResourceContent atomicBinary(final String resourceId, final String parentId, final String content) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.setObjectRoot(true);
        headers.setArchivalGroup(false);
        headers.setInteractionModel(InteractionModel.NON_RDF.getUri());
        headers.setMimeType("text/plain");

        return new ResourceContent(stream(content), headers);
    }

    private ResourceContent binary(final String resourceId, final String parentId, final String content) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.setObjectRoot(false);
        headers.setArchivalGroup(false);
        headers.setInteractionModel(InteractionModel.NON_RDF.getUri());
        headers.setMimeType("text/plain");

        return new ResourceContent(stream(content), headers);
    }

    private ResourceContent atomicContainer(final String resourceId, final String parentId, final String content) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.setObjectRoot(true);
        headers.setArchivalGroup(false);
        headers.setInteractionModel(InteractionModel.BASIC_CONTAINER.getUri());
        headers.setMimeType("text/turtle");

        return new ResourceContent(stream(content), headers);
    }

    private ResourceContent container(final String resourceId, final String parentId, final String content) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.setObjectRoot(false);
        headers.setArchivalGroup(false);
        headers.setInteractionModel(InteractionModel.BASIC_CONTAINER.getUri());
        headers.setMimeType("text/turtle");

        return new ResourceContent(stream(content), headers);
    }

    private ResourceContent ag(final String resourceId, final String parentId, final String content) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.setObjectRoot(true);
        headers.setArchivalGroup(true);
        headers.setInteractionModel(InteractionModel.BASIC_CONTAINER.getUri());
        headers.setMimeType("text/turtle");

        return new ResourceContent(stream(content), headers);
    }

    private ResourceContent acl(final String resourceId, final String parentId, final String content) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.setObjectRoot(false);
        headers.setArchivalGroup(false);
        headers.setInteractionModel(InteractionModel.ACL.getUri());
        headers.setMimeType("text/turtle");

        return new ResourceContent(stream(content), headers);
    }

    private ResourceContent desc(final String resourceId, final String parentId, final String content) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.setObjectRoot(false);
        headers.setArchivalGroup(false);
        headers.setInteractionModel(InteractionModel.NON_RDF_DESCRIPTION.getUri());
        headers.setMimeType("text/turtle");

        return new ResourceContent(stream(content), headers);
    }

    private ResourceHeaders defaultHeaders(final String resourceId, final String parentId, final String content) {
        final var headers = new ResourceHeaders();
        headers.setId(resourceId);
        headers.setParent(parentId);
        headers.setCreatedBy(DEFAULT_USER);
        headers.setCreatedDate(Instant.now());
        headers.setLastModifiedBy(DEFAULT_USER);
        headers.setLastModifiedDate(Instant.now());
        if (content != null) {
            headers.setContentSize((long) content.length());
        }
        return headers;
    }

    private InputStream stream(final String value) {
        if (value == null) {
            return null;
        }
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }

    private String toString(final Optional<InputStream> stream) {
        try (var is = stream.get()) {
            return new String(is.readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void write(final OcflObjectSession session, final ResourceContent content) {
        session.writeResource(content.getHeaders(), content.getContentStream().orElse(null));
    }

    private void close(final ResourceContent content) {
        try {
            content.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
