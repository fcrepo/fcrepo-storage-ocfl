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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import org.fcrepo.storage.ocfl.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

/**
 * Default OcflObjectSessionFactory implementation
 *
 * @author pwinckles
 */
public class DefaultOcflObjectSessionFactory implements OcflObjectSessionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOcflObjectSessionFactory.class);

    private final MutableOcflRepository ocflRepo;
    private final Path stagingRoot;
    private final ObjectReader headerReader;
    private final ObjectWriter headerWriter;
    private final Cache<String, ResourceHeaders> headersCache;
    private CommitType defaultCommitType;
    private final String defaultVersionMessage;
    private final String defaultVersionUserName;
    private final String defaultVersionUserAddress;

    private boolean closed = false;

    public DefaultOcflObjectSessionFactory(final MutableOcflRepository ocflRepo,
                                           final Path stagingRoot,
                                           final ObjectMapper objectMapper,
                                           final Cache<String, ResourceHeaders> headersCache,
                                           final CommitType defaultCommitType,
                                           final String defaultVersionMessage,
                                           final String defaultVersionUserName,
                                           final String defaultVersionUserAddress) {
        this.ocflRepo = Objects.requireNonNull(ocflRepo, "ocflRepo cannot be null");
        this.stagingRoot = Objects.requireNonNull(stagingRoot, "stagingRoot cannot be null");
        this.headerReader = Objects.requireNonNull(objectMapper, "objectMapper cannot be null")
                .readerFor(ResourceHeaders.class);
        this.headerWriter = objectMapper.writerFor(ResourceHeaders.class);
        this.headersCache = headersCache;
        this.defaultCommitType = Objects.requireNonNull(defaultCommitType, "defaultCommitType cannot be null");
        this.defaultVersionMessage = defaultVersionMessage;
        this.defaultVersionUserName = defaultVersionUserName;
        this.defaultVersionUserAddress = defaultVersionUserAddress;
    }

    @Override
    public OcflObjectSession newSession(final String ocflObjectId) {
        enforceOpen();

        final var sessionId = UUID.randomUUID().toString();
        final var session = new DefaultOcflObjectSession(
                sessionId,
                ocflRepo,
                ocflObjectId,
                stagingRoot.resolve(sessionId),
                headerReader,
                headerWriter,
                defaultCommitType,
                headersCache
        );

        session.versionAuthor(defaultVersionUserName, defaultVersionUserAddress);
        session.versionMessage(defaultVersionMessage);

        return session;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            ocflRepo.close();
        }
    }

    private void enforceOpen() {
        if (closed) {
            throw new IllegalStateException("The session factory is closed!");
        }
    }

    /**
     * Allows the default CommitType to be changed at run time -- useful for testing.
     *
     * @param defaultCommitType commit type
     */
    public void setDefaultCommitType(final CommitType defaultCommitType) {
        this.defaultCommitType = Objects.requireNonNull(defaultCommitType, "defaultCommitType cannot be null");
    }

}
