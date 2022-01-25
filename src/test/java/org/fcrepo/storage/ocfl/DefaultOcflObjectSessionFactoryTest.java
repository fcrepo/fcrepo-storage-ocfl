/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import edu.wisc.library.ocfl.core.path.mapper.LogicalPathMappers;
import edu.wisc.library.ocfl.core.storage.OcflStorageBuilder;
import org.apache.commons.lang3.SystemUtils;
import org.fcrepo.storage.ocfl.cache.NoOpCache;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static org.junit.Assert.assertNotEquals;

/**
 * @author pwinckles
 */
public class DefaultOcflObjectSessionFactoryTest {

    @Rule
    public TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    private Path ocflRoot;
    private Path sessionStaging;

    private MutableOcflRepository ocflRepo;
    private OcflObjectSessionFactory sessionFactory;

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
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .logicalPathMapper(logicalPathMapper)
                .storage(OcflStorageBuilder.builder().fileSystem(ocflRoot).build())
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
                new NoOpCache<>(),
                CommitType.NEW_VERSION, DEFAULT_MESSAGE, DEFAULT_USER, DEFAULT_ADDRESS);
    }

    @Test
    public void returnDifferentSessionsForTheSameObject() {
        final var session1 = sessionFactory.newSession("obj1");
        final var session2 = sessionFactory.newSession("obj1");

        assertNotEquals(session1.sessionId(), session2.sessionId());
    }

}
