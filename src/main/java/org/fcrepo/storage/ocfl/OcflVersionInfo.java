/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
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
