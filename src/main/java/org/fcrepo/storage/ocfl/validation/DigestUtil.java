/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl.validation;

import org.apache.commons.codec.binary.Hex;
import org.fcrepo.storage.ocfl.exception.ChecksumMismatchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author pwinckles
 */
final class DigestUtil {

    private static final Logger LOG = LoggerFactory.getLogger(DigestUtil.class);

    private static final Map<String, String> ALGO_MAP = Map.of(
            "sha1", "SHA-1",
            "sha256", "SHA-256",
            "sha512", "SHA-512",
            "sha512/256", "SHA-512/256",
            "md5", "MD5"
    );

    private DigestUtil() {

    }

    /**
     * Verifies that the digests of the input stream match all of the expectations.
     *
     * @param stream the stream to compute digests of
     * @param expectedDigests the expected digests in the form urn:ALGORITHM:DIGEST
     * @throws ChecksumMismatchException when one or more digests do not match the expectation
     */
    public static void checkFixity(final InputStream stream, final Collection<URI> expectedDigests) {
        final var expectedMap = parseDigests(expectedDigests);

        if (expectedMap.isEmpty()) {
            return;
        }

        final var streamMap = new HashMap<String, DigestInputStream>();

        var wrappedStream = stream;

        for (var algorithm : expectedMap.keySet()) {
            final var digestStream = digestInputStream(wrappedStream, algorithm);
            streamMap.put(algorithm, digestStream);
            wrappedStream = digestStream;
        }

        try {
            while (wrappedStream.read() != -1) {
                // read the entire stream, ignoring content to calculate digests
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to calculate stream digests", e);
        }

        final var failures = new ArrayList<String>();

        streamMap.forEach((algorithm, digestStream) -> {
            final var expected = expectedMap.get(algorithm);
            final var actual = Hex.encodeHexString(digestStream.getMessageDigest().digest());
            if (!expected.equalsIgnoreCase(actual)) {
                failures.add(String.format("%s fixity check failed. Expected: %s; Actual: %s",
                        algorithm, expected, actual));
            }
        });

        if (!failures.isEmpty()) {
            throw new ChecksumMismatchException(failures);
        }
    }

    private static Map<String, String> parseDigests(final Collection<URI> digests) {
        final var map = new HashMap<String, String>();

        for (var digest : digests) {
            final var parts = digest.toString().split(":");

            if (parts.length != 3) {
                LOG.debug("Skipping invalid digest: {}", digest);
                continue;
            }

            final var algorithm = ALGO_MAP.get(parts[1].toLowerCase().replaceAll("-", ""));

            if (algorithm == null) {
                LOG.debug("Skipping invalid digest algorithm: {}", digest);
                continue;
            }

            map.put(algorithm, parts[2]);
        }

        return map;
    }

    private static DigestInputStream digestInputStream(final InputStream stream, final String algorithm) {
        try {
            final var messageDigest = MessageDigest.getInstance(algorithm);
            return new DigestInputStream(stream, messageDigest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to create message digest for " + algorithm, e);
        }
    }

}
