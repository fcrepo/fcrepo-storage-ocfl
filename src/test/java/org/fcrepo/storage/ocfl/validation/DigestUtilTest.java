/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl.validation;

import org.apache.commons.io.IOUtils;
import org.fcrepo.storage.ocfl.exception.ChecksumMismatchException;
import org.junit.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;

/**
 * @author pwinckles
 */
public class DigestUtilTest {

    @Test
    public void passFixityCheckWhenMultipleExpectedDigestsAllMatch() {
        DigestUtil.checkFixity(IOUtils.toInputStream("test", StandardCharsets.UTF_8), List.of(
                digest("md5", "098f6bcd4621d373cade4e832627b4f6"),
                digest("sha1", "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"),
                digest("sha256", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"),
                digest("sha512", "ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db27ac" +
                        "185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff"),
                digest("sha512/256", "3d37fe58435e0d87323dee4a2c1b339ef954de63716ee79f5747f94d974f913f")
            ));
    }

    @Test
    public void failFixityCheckWhenMultipleExpectedDigestsNotAllMatch() {
        try {
            DigestUtil.checkFixity(IOUtils.toInputStream("test", StandardCharsets.UTF_8), List.of(
                    digest("md5", "098f6bcd4621d373cade4e832627b4f6"),
                    digest("sha1", "mismatch"),
                    digest("sha256", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"),
                    digest("sha512", "mismatch"),
                    digest("sha512/256", "3d37fe58435e0d87323dee4a2c1b339ef954de63716ee79f5747f94d974f913f")
            ));
            fail("Fixit check should have thrown an exception");
        } catch (ChecksumMismatchException e) {
            assertThat(e.getProblems(), containsInAnyOrder(
                    containsString("SHA-1 fixity check failed. Expected: mismatch;" +
                            " Actual: a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"),
                    containsString("SHA-512 fixity check failed. Expected: mismatch;" +
                            " Actual: ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db27ac" +
                            "185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff")
            ));
        }
    }

    @Test
    public void supportAllCapsAlgorithms() {
        DigestUtil.checkFixity(IOUtils.toInputStream("test", StandardCharsets.UTF_8), List.of(
                digest("MD5", "098f6bcd4621d373cade4e832627b4f6"),
                digest("SHA1", "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"),
                digest("SHA256", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"),
                digest("SHA512", "ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db27ac" +
                        "185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff"),
                digest("SHA512/256", "3d37fe58435e0d87323dee4a2c1b339ef954de63716ee79f5747f94d974f913f")
        ));
    }

    @Test
    public void supportAllDashAlgorithms() {
        DigestUtil.checkFixity(IOUtils.toInputStream("test", StandardCharsets.UTF_8), List.of(
                digest("md-5", "098f6bcd4621d373cade4e832627b4f6"),
                digest("sha-1", "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"),
                digest("sha-256", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"),
                digest("sha-512", "ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db27ac" +
                        "185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff"),
                digest("sha-512/256", "3d37fe58435e0d87323dee4a2c1b339ef954de63716ee79f5747f94d974f913f")
        ));
    }

    @Test
    public void skipUnknownAlgorithms() {
        DigestUtil.checkFixity(IOUtils.toInputStream("test", StandardCharsets.UTF_8), List.of(
                digest("blake2b", "098f6bcd4621d373cade4e832627b4f6")));
    }

    @Test
    public void skipBadDigests() {
        DigestUtil.checkFixity(IOUtils.toInputStream("test", StandardCharsets.UTF_8),
                List.of(URI.create("bogus")));
    }

    private URI digest(final String algorithm, final String value) {
        return URI.create("urn:" + algorithm + ":" + value);
    }

}
