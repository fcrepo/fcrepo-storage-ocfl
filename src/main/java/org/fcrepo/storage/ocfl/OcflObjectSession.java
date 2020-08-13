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
public interface OcflObjectSession {

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
     */
    void writeResource(final ResourceHeaders headers, final InputStream content);

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
     * Reads a resource's header file.
     *
     * @param resourceId the Fedora resource id to read
     * @return the resource's headers
     * @throws NotFoundException if the resource cannot be found
     */
    ResourceHeaders readHeaders(final String resourceId);

    /**
     * Reads a specfic version of a resource's header file.
     *
     * @param resourceId the Fedora resource id to read
     * @param versionNumber the version to read
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
     * @param versionNumber the version to read
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
     * @return true if the session is still open
     */
    boolean isOpen();

}
