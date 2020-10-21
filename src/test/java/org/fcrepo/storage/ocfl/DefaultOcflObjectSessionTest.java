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
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleConfig;
import edu.wisc.library.ocfl.core.path.mapper.LogicalPathMappers;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.SystemUtils;
import org.fcrepo.storage.ocfl.cache.CaffeineCache;
import org.fcrepo.storage.ocfl.cache.NoOpCache;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    private OcflObjectSessionFactory cachedSessionFactory;
    private Cache<String, ResourceHeaders> cache;

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

        sessionFactory = new DefaultOcflObjectSessionFactory(ocflRepo,
                sessionStaging,
                objectMapper,
                new NoOpCache<>(),
                CommitType.NEW_VERSION, DEFAULT_MESSAGE, DEFAULT_USER, DEFAULT_ADDRESS);

        cache = Caffeine.newBuilder().maximumSize(100).build();

        cachedSessionFactory = new DefaultOcflObjectSessionFactory(ocflRepo,
                sessionStaging,
                objectMapper,
                new CaffeineCache<>(cache),
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
        final var binaryContent = binary(binaryId, containerId, agId, "baz");

        write(session, agContent);
        write(session, containerContent);
        write(session, binaryContent);

        final var stagedContent = session.readContent(binaryId);

        assertResourceContent("baz", binaryContent, stagedContent);

        session.commit();

        final var correctAg = touch(agContent, binaryContent);

        final var committedAg = session.readContent(agId);
        final var committedContainer = session.readContent(containerId);
        final var committedBinary = session.readContent(binaryId);

        assertResourceContent("foo", correctAg, committedAg);
        assertResourceContent("bar", containerContent, committedContainer);
        assertResourceContent("baz", binaryContent, committedBinary);
    }

    @Test
    public void cacheTest() {
        final var agId = "info:fedora/foo";
        final var containerId = "info:fedora/foo/bar";
        final var binaryId = "info:fedora/foo/bar/baz";

        final var session = cachedSessionFactory.newSession(agId);

        final var agContent = ag(agId, ROOT, "foo");
        final var containerContent = container(containerId, agId, "bar");
        final var binaryContent = binary(binaryId, containerId, agId, "baz");

        write(session, agContent);
        write(session, containerContent);
        write(session, binaryContent);
        final var correctAg = touch(agContent, binaryContent);

        final var stagedContent = session.readContent(binaryId);

        assertResourceContent("baz", binaryContent, stagedContent);

        assertEquals(0, cache.estimatedSize());

        session.commit();

        assertEquals(3, cache.estimatedSize());
        assertTrue(cache.asMap().containsKey(agId + "_v1"));
        assertTrue(cache.asMap().containsKey(containerId + "_v1"));
        assertTrue(cache.asMap().containsKey(binaryId + "_v1"));

        final var committedAg = session.readContent(agId);
        final var committedContainer = session.readContent(containerId);
        final var committedBinary = session.readContent(binaryId);

        assertResourceContent("foo", correctAg, committedAg);
        assertResourceContent("bar", containerContent, committedContainer);
        assertResourceContent("baz", binaryContent, committedBinary);
    }

    @Test
    public void writeNewBinaryAcl() {
        final var resourceId = "info:fedora/foo/bar";
        final var aclId = "info:fedora/foo/bar/fcr:acl";

        final var session = sessionFactory.newSession(resourceId);

        final var content = atomicBinary(resourceId, "info:fedora/foo", "blah");
        final var acl = acl(false, aclId, resourceId, "acl");

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
        final var acl = acl(true, aclId, resourceId, "acl");

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

        final var deleteHeaders = ResourceHeaders.builder(existing.getHeaders())
                .withContentPath(null)
                .withContentSize(null)
                .withDigests(null)
                .withDeleted(true)
                .build();

        session1.deleteContentFile(deleteHeaders);

        final var stagedDelete = session1.readContent(DEFAULT_AG_BINARY_ID);

        assertEquals(deleteHeaders, stagedDelete.getHeaders());
        assertFalse(stagedDelete.getContentStream().isPresent());

        session1.commit();

        final var deleted = session1.readContent(DEFAULT_AG_BINARY_ID);

        assertEquals(deleteHeaders, deleted.getHeaders());
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
        final var correctAg = touch(ag, binary);

        session.deleteContentFile(binary.getHeaders());

        expectResourceNotFound(DEFAULT_AG_BINARY_ID, session);

        session.commit();

        expectResourceNotFound(DEFAULT_AG_BINARY_ID, session);
        assertResourceContent("ag", correctAg, session.readContent(DEFAULT_AG_ID));
    }

    @Test
    public void deleteFileAndThenReAddBeforeCommitting() {
        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        final var ag = ag(DEFAULT_AG_ID, ROOT, "ag");
        final var binary = binary(DEFAULT_AG_BINARY_ID, DEFAULT_AG_ID, "binary");
        final var binary2 = binary(DEFAULT_AG_BINARY_ID, DEFAULT_AG_ID, "binary2");
        final var correctAg = touch(ag, binary2);

        write(session, ag);
        write(session, binary);

        session.deleteContentFile(binary.getHeaders());
        expectResourceNotFound(DEFAULT_AG_BINARY_ID, session);

        write(session, binary2);

        session.commit();

        assertResourceContent("ag", correctAg, session.readContent(DEFAULT_AG_ID));
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

        final var deleteHeaders = ResourceHeaders.builder(ag.getHeaders())
                .withDeleted(true)
                .withContentPath(null)
                .withContentSize(null)
                .withDigests(null)
                .build();

        session.deleteContentFile(deleteHeaders);
        session.commit();

        final var deletedContent = session.readContent(DEFAULT_AG_ID);

        assertEquals(deleteHeaders, deletedContent.getHeaders());
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
        final var session = sessionFactory.newSession(resourceId);

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

        assertVersions(session3.listVersions(DEFAULT_AG_ID), "v1", "v2", "v3");
        assertVersions(session3.listVersions(DEFAULT_AG_BINARY_ID), "v1", "v3");
        assertVersions(session3.listVersions(binary2Id), "v2");
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
        final var content = atomicBinary(resourceId, ROOT, "bar", headers -> {
            headers.withDigests(List.of(
                    URI.create("urn:sha-512:dc6b68d13b8cf959644b935f1192b02c71aa7a5cf653bd43b4480fa89eec8d4d3f16a" +
                            "2278ec8c3b40ab1fdb233b3173a78fd83590d6f739e0c9e8ff56c282557")));
        });

        final var session = sessionFactory.newSession(resourceId);

        write(session, content);
        session.commit();
    }

    @Test
    public void addOcflDigestWhenNotProvided() throws URISyntaxException {
        final var resourceId = "info:fedora/foo";

        final var sha512Digest = "d82c4eb5261cb9c8aa9855edd67d1bd10482f41529858d925094d173fa662aa" +
                "91ff39bc5b188615273484021dfb16fd8284cf684ccf0fc795be3aa2fc1e6c181";
        final var sha256Digest = "fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9";

        final var digests = new ArrayList<URI>();
        digests.add(new URI("urn:sha-256:" + sha256Digest));

        final var content = atomicBinary(resourceId, ROOT, "bar", headers -> {
            headers.withDigests(digests);
        });

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
        // Correct ag last modified and state token to binary child creation.
        final var correctAg = touch(ag, binary);

        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        final var resources = listHeaders(session);

        assertHeaders(resources, correctAg.getHeaders(), binary.getHeaders());
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

        // Match ag last modified and state token to binary child update.
        final var correctAg = touch(ag, binaryUpdate);

        final var resources = listHeaders(session);

        assertHeaders(resources, correctAg.getHeaders(), binaryUpdate.getHeaders());
    }

    @Test
    public void listResourcesWhenMultipleWithStagedDeletes() {
        final var ag = defaultAg();
        final var binary = defaultAgBinary();
        close(ag);
        close(binary);

        final var session = sessionFactory.newSession(DEFAULT_AG_ID);

        session.deleteContentFile(binary.getHeaders());
        // Match ag lastmodified and state token to binary child.
        final var correctAg1 = touch(ag, binary);

        final var deleteHeaders = ResourceHeaders.builder(binary.getHeaders())
                .withContentPath(null)
                .withContentSize(null)
                .withDigests(null)
                .build();

        final var resources = listHeaders(session);
        assertHeaders(resources, correctAg1.getHeaders(), deleteHeaders);

        session.deleteResource(DEFAULT_AG_BINARY_ID);

        // Match ag lastmodified and state token to deletion of binary child.
        final var correctAg2 = touch(ag, deleteHeaders);
        final var resources2 = listHeaders(session);
        assertHeaders(resources2, correctAg2.getHeaders());
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
    public void touchAgWhenAgPartUpdated() {
        close(defaultAg());

        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        assertVersions(session.listVersions(DEFAULT_AG_ID), "v1");

        close(defaultAgBinary());

        assertVersions(session.listVersions(DEFAULT_AG_ID), "v1", "v2");
        assertVersions(session.listVersions(DEFAULT_AG_BINARY_ID), "v2");
    }

    @Test
    public void touchingShouldUseTheTimestampFromTheLastUpdatedResource() throws InterruptedException {
        close(defaultAg());

        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        assertVersions(session.listVersions(DEFAULT_AG_ID), "v1");

        final var binary = binary(DEFAULT_AG_BINARY_ID, DEFAULT_AG_ID, "bar");
        final var timestamp = binary.getHeaders().getLastModifiedDate();

        TimeUnit.SECONDS.sleep(1);

        final var session2 = sessionFactory.newSession(DEFAULT_AG_ID);
        write(session2, binary);
        session2.commit();

        final var agHeaders = session2.readHeaders(DEFAULT_AG_ID);
        assertEquals(timestamp, agHeaders.getLastModifiedDate());
    }

    @Test
    public void doNotTouchAgWhenAclUpdated() {
        close(defaultAg());

        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        assertVersions(session.listVersions(DEFAULT_AG_ID), "v1");

        final var resourceId = DEFAULT_AG_ID + "/fcr:acl";
        final var content = acl(true, resourceId, DEFAULT_AG_ID, "blah");

        write(session, content);
        session.commit();

        assertVersions(session.listVersions(DEFAULT_AG_ID), "v1");
        assertVersions(session.listVersions(resourceId), "v2");
    }

    @Test
    public void touchBinaryWhenDescUpdated() {
        final var resourceId = "info:fedora/foo";
        final var content = atomicBinary(resourceId, ROOT, "foo");

        final var descId = "info:fedora/foo/fcr:metadata";
        final var descContent = desc(descId, resourceId, "desc");

        final var session = sessionFactory.newSession(resourceId);

        write(session, content);
        write(session, descContent);
        session.commit();

        assertVersions(session.listVersions(resourceId), "v1");
        assertVersions(session.listVersions(descId), "v1");

        final var session2 = sessionFactory.newSession(resourceId);
        final var descContent2 = desc(descId, resourceId, "desc2");

        write(session2, descContent2);
        session2.commit();

        assertVersions(session.listVersions(resourceId), "v1", "v2");
        assertVersions(session.listVersions(descId), "v1", "v2");
    }

    @Test
    public void touchBinaryDescWhenBinaryUpdated() {
        final var resourceId = "info:fedora/foo";
        final var content = atomicBinary(resourceId, ROOT, "foo");

        final var descId = "info:fedora/foo/fcr:metadata";
        final var descContent = desc(descId, resourceId, "desc");

        final var session = sessionFactory.newSession(resourceId);

        write(session, content);
        write(session, descContent);
        session.commit();

        assertVersions(session.listVersions(resourceId), "v1");
        assertVersions(session.listVersions(descId), "v1");

        final var session2 = sessionFactory.newSession(resourceId);
        final var content2 = atomicBinary(resourceId, ROOT, "bar");

        write(session2, content2);
        session2.commit();

        assertVersions(session.listVersions(resourceId), "v1", "v2");
        assertVersions(session.listVersions(descId), "v1", "v2");
    }

    @Test
    public void failWriteWhenContentSizeDoesNotMatch() {
        final var resourceId = "info:fedora/foo";
        final var content = atomicBinary(resourceId, ROOT, "first", headers -> {
            headers.withContentSize(1024L);
        });

        final var session = sessionFactory.newSession(resourceId);

        try {
            write(session, content);
            fail("should have thrown an exception");
        } catch (InvalidContentException e) {
            assertThat(e.getMessage(), containsString("file size does not match"));
        }
    }

    @Test
    public void contentWriteFailuresShouldCleanupStagedFiles() throws IOException {
        final var resourceId = "info:fedora/foo";
        final var content = atomicBinary(resourceId, ROOT, "first", headers -> {
            headers.withContentSize(1024L);
        });

        final var session = sessionFactory.newSession(resourceId);

        try {
            write(session, content);
            fail("should have thrown an exception");
        } catch (RuntimeException e) {
            assertThat(recursiveList(sessionStaging), empty());
        }
    }

    @Test
    public void versioningMutableHeadAgShouldVersionPartsWithStagedChanges() {
        final var agId = "info:fedora/foo";
        final var containerId = "info:fedora/foo/bar";
        final var binaryId = "info:fedora/foo/bar/baz";
        final var binary2Id = "info:fedora/foo/bar/boz";

        // Create initial resources -- mutable head

        final var session = sessionFactory.newSession(agId);
        session.commitType(CommitType.UNVERSIONED);

        final var agContent = ag(agId, ROOT, "foo");
        final var containerContent = container(containerId, agId, "bar");
        final var binaryContent = binary(binaryId, containerId, "baz");
        final var binary2Content = binary(binary2Id, containerId, "boz");

        write(session, agContent);
        write(session, containerContent);
        write(session, binaryContent);
        write(session, binary2Content);

        session.commit();

        assertEquals(0, session.listVersions(agId).size());
        assertEquals(0, session.listVersions(containerId).size());
        assertEquals(0, session.listVersions(binaryId).size());
        assertEquals(0, session.listVersions(binary2Id).size());

        // Commit mutable head

        final var session2 = sessionFactory.newSession(agId);
        session2.commit();

        assertVersions(session2.listVersions(agId), "v2");
        assertVersions(session2.listVersions(containerId), "v2");
        assertVersions(session2.listVersions(binaryId), "v2");
        assertVersions(session2.listVersions(binary2Id), "v2");

        // Update a subset of resources -- mutable head

        final var session3 = sessionFactory.newSession(agId);
        session3.commitType(CommitType.UNVERSIONED);

        final var containerContentV2 = container(containerId, agId, "bar - 2");
        final var binaryContentV2 = binary(binaryId, containerId, "baz - 2");

        write(session3, containerContentV2);
        write(session3, binaryContentV2);

        session3.commit();

        assertVersions(session3.listVersions(agId), "v2");
        assertVersions(session3.listVersions(containerId), "v2");
        assertVersions(session3.listVersions(binaryId), "v2");
        assertVersions(session3.listVersions(binary2Id), "v2");

        // Update a subset of resources again -- mutable head

        final var session4 = sessionFactory.newSession(agId);
        session4.commitType(CommitType.UNVERSIONED);

        final var containerContentV3 = container(containerId, agId, "bar - 3");

        write(session4, containerContentV3);

        session4.commit();

        assertVersions(session4.listVersions(agId), "v2");
        assertVersions(session4.listVersions(containerId), "v2");
        assertVersions(session4.listVersions(binaryId), "v2");
        assertVersions(session4.listVersions(binary2Id), "v2");

        // Commit mutable head

        final var session5 = sessionFactory.newSession(agId);
        session5.commit();

        assertVersions(session5.listVersions(agId), "v2", "v3");
        assertVersions(session5.listVersions(containerId), "v2", "v3");
        assertVersions(session5.listVersions(binaryId), "v2", "v3");
        assertVersions(session5.listVersions(binary2Id), "v2");
    }

    @Test
    public void testDeleteChildInAgUpdatesAg() throws Exception {
        // Create AG and child resource
        final var ag = defaultAg();
        final var binaryChild = defaultAgBinary();
        close(ag);
        close(binaryChild);
        // Time passes
        Thread.sleep(1000);

        final var session = sessionFactory.newSession(DEFAULT_AG_ID);
        final var now = Instant.now();
        // Mark the child as deleted
        final var deleteHeaders = ResourceHeaders.builder(binaryChild.getHeaders())
                .withContentPath(null)
                .withContentSize(null)
                .withDigests(null)
                .withDeleted(true)
                .withLastModifiedDate(now)
                .withStateToken(getStateToken(now))
                .build();
        session.deleteContentFile(deleteHeaders);

        // The AG should have an updated timestamp to indicate it was altered
        final var correctAg = touch(ag, deleteHeaders);
        final var resources = listHeaders(session);
        assertHeaders(resources, correctAg.getHeaders(), deleteHeaders);
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

    private ResourceContent atomicBinary(final String resourceId,
                                         final String parentId,
                                         final String content,
                                         final Consumer<ResourceHeaders.Builder> modifyHeaders) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.withObjectRoot(true);
        headers.withArchivalGroup(false);
        headers.withInteractionModel(InteractionModel.NON_RDF.getUri());
        headers.withMimeType("text/plain");

        if (content != null) {
            headers.withContentPath(PersistencePaths.nonRdfResource(resourceId, resourceId).getContentFilePath());
        }

        if (modifyHeaders != null) {
            modifyHeaders.accept(headers);
        }

        return new ResourceContent(stream(content), headers.build());
    }

    private ResourceContent atomicBinary(final String resourceId, final String parentId, final String content) {
        return atomicBinary(resourceId, parentId, content, null);
    }

    private ResourceContent binary(final String resourceId, final String parentId, final String content) {
        return binary(resourceId, parentId, parentId, content);
    }

    private ResourceContent binary(final String resourceId,
                                   final String parentId,
                                   final String rootResourceId,
                                   final String content) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.withObjectRoot(false);
        headers.withArchivalGroup(false);
        headers.withInteractionModel(InteractionModel.NON_RDF.getUri());
        headers.withMimeType("text/plain");
        headers.withContentPath(PersistencePaths.nonRdfResource(rootResourceId, resourceId).getContentFilePath());
        return new ResourceContent(stream(content), headers.build());
    }

    private ResourceContent atomicContainer(final String resourceId, final String parentId, final String content) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.withObjectRoot(true);
        headers.withArchivalGroup(false);
        headers.withInteractionModel(InteractionModel.BASIC_CONTAINER.getUri());
        headers.withMimeType("text/turtle");
        headers.withContentPath(PersistencePaths.rdfResource(resourceId, resourceId).getContentFilePath());
        return new ResourceContent(stream(content), headers.build());
    }

    private ResourceContent container(final String resourceId, final String parentId, final String content) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.withObjectRoot(false);
        headers.withArchivalGroup(false);
        headers.withInteractionModel(InteractionModel.BASIC_CONTAINER.getUri());
        headers.withMimeType("text/turtle");
        headers.withContentPath(PersistencePaths.rdfResource(parentId, resourceId).getContentFilePath());
        return new ResourceContent(stream(content), headers.build());
    }

    private ResourceContent ag(final String resourceId, final String parentId, final String content) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.withObjectRoot(true);
        headers.withArchivalGroup(true);
        headers.withInteractionModel(InteractionModel.BASIC_CONTAINER.getUri());
        headers.withMimeType("text/turtle");
        headers.withContentPath(PersistencePaths.rdfResource(resourceId, resourceId).getContentFilePath());
        return new ResourceContent(stream(content), headers.build());
    }

    private ResourceContent acl(final boolean describesRdf,
                                final String resourceId,
                                final String parentId,
                                final String content) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.withObjectRoot(false);
        headers.withArchivalGroup(false);
        headers.withInteractionModel(InteractionModel.ACL.getUri());
        headers.withMimeType("text/turtle");
        headers.withContentPath(PersistencePaths.aclResource(describesRdf, parentId, resourceId).getContentFilePath());
        return new ResourceContent(stream(content), headers.build());
    }

    private ResourceContent desc(final String resourceId, final String parentId, final String content) {
        final var headers = defaultHeaders(resourceId, parentId, content);
        headers.withObjectRoot(false);
        headers.withArchivalGroup(false);
        headers.withInteractionModel(InteractionModel.NON_RDF_DESCRIPTION.getUri());
        headers.withMimeType("text/turtle");
        headers.withContentPath(PersistencePaths.rdfResource(parentId, resourceId).getContentFilePath());
        return new ResourceContent(stream(content), headers.build());
    }

    private ResourceHeaders.Builder defaultHeaders(final String resourceId,
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

    private String getStateToken(final Instant timestamp) {
        return DigestUtils.md5Hex(String.valueOf(timestamp.toEpochMilli())).toUpperCase();
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

    private List<Path> recursiveList(final Path root) {
        try (var walk = Files.walk(root)) {
            return walk.filter(Files::isRegularFile).collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void assertHeaders(final List<ResourceHeaders> actual, final ResourceHeaders... expected) {
        final var expectedArray = Arrays.stream(expected).toArray(ResourceHeaders[]::new);
        assertThat(actual, containsInAnyOrder(expectedArray));
    }

    private List<ResourceHeaders> listHeaders(final OcflObjectSession session) {
        return session.streamResourceHeaders().collect(Collectors.toList());
    }

    private void assertVersions(final List<OcflVersionInfo> actual, final String... expected) {
        final var versionNums = actual.stream()
                .map(OcflVersionInfo::getVersionNumber)
                .collect(Collectors.toList());
        assertThat(versionNums, contains(expected));
    }

    /**
     * Align the lastModifiedDate and stateToken to that of the related headers.
     * @param modifiedResource the resource to modify
     * @param relatedHeaders the resource headers to use for the modifying values.
     * @return the modifiedResource with new lastModifiedDate and stateToken headers.
     */
    private ResourceContent touch(final ResourceContent modifiedResource, final ResourceHeaders relatedHeaders) {
        final var newHeaders = ResourceHeaders.builder(modifiedResource.getHeaders())
                .withLastModifiedDate(relatedHeaders.getLastModifiedDate())
                .withStateToken(relatedHeaders.getStateToken()).build();
        return new ResourceContent(modifiedResource.getContentStream(), newHeaders);
    }

    private ResourceContent touch(final ResourceContent modifiedResource, final ResourceContent relatedResource) {
        return touch(modifiedResource, relatedResource.getHeaders());
    }
}
