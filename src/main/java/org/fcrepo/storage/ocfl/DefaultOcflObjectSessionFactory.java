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
import edu.wisc.library.ocfl.api.OcflRepository;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default OcflObjectSessionFactory implementation
 *
 * @author pwinckles
 */
public class DefaultOcflObjectSessionFactory implements OcflObjectSessionFactory {

    private final OcflRepository ocflRepo;
    private final Path stagingRoot;
    private final ObjectMapper objectMapper;
    private final String defaultVersionMessage;
    private final String defaultVersionUserName;
    private final String defaultVersionUserAddress;

    private final Map<String, OcflObjectSession> sessions;

    public DefaultOcflObjectSessionFactory(final OcflRepository ocflRepo,
                                           final Path stagingRoot,
                                           final ObjectMapper objectMapper,
                                           final String defaultVersionMessage,
                                           final String defaultVersionUserName,
                                           final String defaultVersionUserAddress) {
        this.ocflRepo = ocflRepo;
        this.stagingRoot = stagingRoot;
        this.objectMapper = objectMapper;
        this.defaultVersionMessage = defaultVersionMessage;
        this.defaultVersionUserName = defaultVersionUserName;
        this.defaultVersionUserAddress = defaultVersionUserAddress;
        this.sessions = new ConcurrentHashMap<>();
    }

    @Override
    public OcflObjectSession newSession(final String ocflObjectId) {
        final var sessionId = UUID.randomUUID().toString();
        final var session = new DefaultOcflObjectSession(
                sessionId,
                ocflRepo,
                ocflObjectId,
                stagingRoot.resolve(sessionId),
                objectMapper,
                () -> sessions.remove(sessionId)
        );

        session.versionAuthor(defaultVersionUserName, defaultVersionUserAddress);
        session.versionMessage(defaultVersionMessage);

        sessions.put(sessionId, session);
        return session;
    }

    @Override
    public Optional<OcflObjectSession> existingSession(final String sessionId) {
        return Optional.ofNullable(sessions.get(sessionId));
    }

}