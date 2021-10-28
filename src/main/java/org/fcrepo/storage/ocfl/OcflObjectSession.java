/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl;

import org.fcrepo.storage.ocfl.exception.InvalidContentException;
import org.fcrepo.storage.ocfl.exception.NotFoundException;

import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.stream.Stream;

/**
 * Session interface over an OCFL object. Changes to the object are accumulated in a staging directory until the
 * session is committed, at which point all of the changes are written to a new OCFL object version.
 *
 * @author pwinckles
 */
public interface OcflObjectSession extends AutoCloseable {

    /**
     * @return the id of the session
     */
    String sessionId();

    /**
     * @return the OCFL object id of the object the session is on
     */
    String ocflObjectId();

    /**
     * Writes a resource to the session.
     *
     * @param headers the resource's headers
     * @param content the resource's content, may be null if the resource has no content
     * @return the resource headers that were written to disk, these may differ from the input headers
     * @throws InvalidContentException when the content size in the headers does not match the actual content size
     */
    ResourceHeaders writeResource(final ResourceHeaders headers, final InputStream content);

    /**
     * Writes the resources headers to the session.
     *
     * @param headers the headers to write
     */
    void writeHeaders(final ResourceHeaders headers);

    /**
     * Deletes a content file from the session, and updates the associated headers. If the resource was added in
     * the current session, then its headers are also deleted and it is as if the resource never existed.
     *
     * @param headers the updated resource headers
     */
    void deleteContentFile(final ResourceHeaders headers);

    /**
     * Deletes all files associated to the specified resource. If the resource is the root resource of the object,
     * then the object will be deleted.
     *
     * @param resourceId the Fedora resource id of the resource to delete
     */
    void deleteResource(final String resourceId);

    /**
     * Indicates if the resource exists in the session
     *
     * @param resourceId the Fedora resource id
     * @return true if the resource exists in the session
     */
    boolean containsResource(final String resourceId);

    /**
     * Reads a resource's header file.
     *
     * @param resourceId the Fedora resource id to read
     * @return the resource's headers
     * @throws NotFoundException if the resource cannot be found
     */
    ResourceHeaders readHeaders(final String resourceId);

    /**
     * Reads a specific version of a resource's header file.
     *
     * @param resourceId the Fedora resource id to read
     * @param versionNumber the version to read, or null for HEAD
     * @return the resource's headers
     * @throws NotFoundException if the resource cannot be found
     */
    ResourceHeaders readHeaders(final String resourceId, final String versionNumber);

    /**
     * Reads a resource's content.
     *
     * @param resourceId the Fedora resource id to read
     * @return the resource's content
     * @throws NotFoundException if the resource cannot be found
     */
    ResourceContent readContent(final String resourceId);

    /**
     * Reads a specific version of a resource's content.
     *
     * @param resourceId the Fedora resource id to read
     * @param versionNumber the version to read, or null for HEAD
     * @return the resource's content
     * @throws NotFoundException if the resource cannot be found
     */
    ResourceContent readContent(final String resourceId, final String versionNumber);

    /**
     * List all of the versions associated to the resource in chrolological order.
     *
     * @param resourceId the Fedora resource id
     * @return list of versions
     * @throws NotFoundException if the resource cannot be found
     */
    List<OcflVersionInfo> listVersions(final String resourceId);

    /**
     * Returns the headers for all of the resources contained within an OCFL object. The results are unordered.
     *
     * @return resource headers
     */
    Stream<ResourceHeaders> streamResourceHeaders();

    /**
     * Sets the timestamp that's stamped on the OCFL version. If this value is not set, the current system time
     * at the time the version is created is used.
     *
     * @param timestamp version creation timestamp
     */
    void versionCreationTimestamp(final OffsetDateTime timestamp);

    /**
     * Sets the author the OCFL version is attributed to.
     *
     * @param name the author's name
     * @param address the author's address
     */
    void versionAuthor(final String name, final String address);

    /**
     * Sets the OCFL version message.
     *
     * @param message the OCFL version message
     */
    void versionMessage(final String message);

    /**
     * Sets the commit behavior -- create a new version or update the mutable HEAD.
     *
     * @param commitType the commit behavior
     */
    void commitType(final CommitType commitType);

    /**
     * Commits the session, persisting all changes to a new OCFL version.
     */
    void commit();

    /**
     * Aborts the session, abandoning all changes.
     */
    void abort();

    /**
     * Rolls back an already committed session, permanently removing the OCFL object version that was created by the
     * session. Sessions may only be rolled back when auto-versioning in enabled. Calling this method on a session
     * that has not yet been committed does nothing.
     *
     * @throws IllegalStateException if the session cannot be rolled back because manual versioning was used
     */
    void rollback();

    /**
     * @return true if the session is still open
     */
    boolean isOpen();

    @Override
    void close();

}
