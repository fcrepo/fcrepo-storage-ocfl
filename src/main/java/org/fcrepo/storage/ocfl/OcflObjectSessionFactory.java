/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl;

/**
 * Factory for creating OcflObjectSessions.
 *
 * @author pwinckles
 */
public interface OcflObjectSessionFactory {

    /**
     * Creates a new OCFL object session for the specified OCFL object.
     *
     * @param ocflObjectId the OCFL object id to open a session for
     * @return new session
     */
    OcflObjectSession newSession(final String ocflObjectId);

    /**
     * Closes the underlying OCFL repository.
     */
    void close();

    /**
     * When unsafe writes are enabled, files are added to OCFL versions by providing the OCFL client with their
     * digest. The client trusts that the digest is accurate and does not calculate the value for itself. This should
     * increase performance, but will corrupt the object if the digest is incorrect.
     *
     * @param useUnsafeWrite true to use unsafe OCFL writes
     */
    void useUnsafeWrite(boolean useUnsafeWrite);

}
