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

import java.time.Instant;
import java.util.Objects;

/**
 * Contains OCFL version metadata for a resource version.
 *
 * @author pwinckles
 */
public class OcflVersionInfo {

    private final String resourceId;
    private final String ocflObjectId;
    private final String versionNumber;
    private final Instant created;

    public OcflVersionInfo(final String resourceId,
                           final String ocflObjectId,
                           final String versionNumber,
                           final Instant created) {
        this.resourceId = resourceId;
        this.ocflObjectId = ocflObjectId;
        this.versionNumber = versionNumber;
        this.created = created;
    }

    /**
     * @return the resourceId of the resource the version is for
     */
    public String getResourceId() {
        return resourceId;
    }

    /**
     * @return the OCFL object id the resource is in
     */
    public String getOcflObjectId() {
        return ocflObjectId;
    }

    /**
     * @return the OCFL version number of the version
     */
    public String getVersionNumber() {
        return versionNumber;
    }

    /**
     * @return the timestamp when the version was created
     */
    public Instant getCreated() {
        return created;
    }

    @Override
    public String toString() {
        return "OcflVersionInfo{" +
                "resourceId='" + resourceId + '\'' +
                ", ocflObjectId='" + ocflObjectId + '\'' +
                ", versionNumber='" + versionNumber + '\'' +
                ", created=" + created +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final OcflVersionInfo that = (OcflVersionInfo) o;
        return Objects.equals(resourceId, that.resourceId) &&
                Objects.equals(ocflObjectId, that.ocflObjectId) &&
                Objects.equals(versionNumber, that.versionNumber) &&
                Objects.equals(created, that.created);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceId, ocflObjectId, versionNumber, created);
    }

}
