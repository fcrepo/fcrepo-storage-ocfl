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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Resource headers contain metadata about Fedora resources in Fedora 6.
 * <p>
 * This class is based on ResourceHeadersImpl in fcrepo-persistence-common.
 *
 * @author bbpennel
 * @author pwinckles
 */
@JsonDeserialize(builder = ResourceHeaders.Builder.class)
public class ResourceHeaders {

    private final String id;
    private final String parent;
    private final String archivalGroupId;
    private final String stateToken;
    private final String interactionModel;
    private final String mimeType;
    private final String filename;
    private final long contentSize;
    private final Collection<URI> digests;
    private final String externalUrl;
    private final String externalHandling;
    private final Instant createdDate;
    private final String createdBy;
    private final Instant lastModifiedDate;
    private final String lastModifiedBy;
    private final Instant mementoCreatedDate;
    private final boolean archivalGroup;
    private final boolean objectRoot;
    private final boolean deleted;
    private final String contentPath;
    private final String headersVersion;

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(final ResourceHeaders original) {
        return new Builder(original);
    }

    private ResourceHeaders(final Builder builder) {
        this(builder.id,
                builder.parent,
                builder.archivalGroupId,
                builder.stateToken,
                builder.interactionModel,
                builder.mimeType,
                builder.filename,
                builder.contentSize,
                builder.digests,
                builder.externalUrl,
                builder.externalHandling,
                builder.createdDate,
                builder.createdBy,
                builder.lastModifiedDate,
                builder.lastModifiedBy,
                builder.mementoCreatedDate,
                builder.archivalGroup,
                builder.objectRoot,
                builder.deleted,
                builder.contentPath,
                builder.headersVersion);
    }

    private ResourceHeaders(final String id,
                            final String parent,
                            final String archivalGroupId,
                            final String stateToken,
                            final String interactionModel,
                            final String mimeType,
                            final String filename,
                            final long contentSize,
                            final Collection<URI> digests,
                            final String externalUrl,
                            final String externalHandling,
                            final Instant createdDate,
                            final String createdBy,
                            final Instant lastModifiedDate,
                            final String lastModifiedBy,
                            final Instant mementoCreatedDate,
                            final boolean archivalGroup,
                            final boolean objectRoot,
                            final boolean deleted,
                            final String contentPath,
                            final String headersVersion) {
        this.id = id;
        this.parent = parent;
        this.archivalGroupId = archivalGroupId;
        this.stateToken = stateToken;
        this.interactionModel = interactionModel;
        this.mimeType = mimeType;
        this.filename = filename;
        this.contentSize = contentSize;
        if (digests == null) {
            this.digests = Collections.emptyList();
        } else {
            // This is necessary so that ResourceHeaders.equals() still works as expected in tests
            if (digests instanceof List) {
                this.digests = Collections.unmodifiableList((List<URI>) digests);
            } else if (digests instanceof Set) {
                this.digests = Collections.unmodifiableSet((Set<URI>) digests);
            } else {
                this.digests = Collections.unmodifiableCollection(digests);
            }
        }
        this.externalUrl = externalUrl;
        this.externalHandling = externalHandling;
        this.createdDate = createdDate;
        this.createdBy = createdBy;
        this.lastModifiedDate = lastModifiedDate;
        this.lastModifiedBy = lastModifiedBy;
        this.mementoCreatedDate = mementoCreatedDate;
        this.archivalGroup = archivalGroup;
        this.objectRoot = objectRoot;
        this.deleted = deleted;
        this.contentPath = contentPath;
        this.headersVersion = headersVersion;
    }

    /**
     * @return the fedora id
     */
    public String getId() {
        return id;
    }

    /**
     * @return the parent fedora id
     */
    public String getParent() {
        return parent;
    }

    /**
     * @return the fedora id of the archival group resource that contains this resource, or null
     */
    public String getArchivalGroupId() {
        return archivalGroupId;
    }

    /**
     * @return the stateToken
     */
    public String getStateToken() {
        return stateToken;
    }

    /**
     * @return the interactionModel
     */
    public String getInteractionModel() {
        return interactionModel;
    }

    /**
     * @return the mimeType
     */
    public String getMimeType() {
        return mimeType;
    }

    /**
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * Content size may be -1 when there is no content or the content's size is unknown
     *
     * @return the contentSize
     */
    public long getContentSize() {
        return contentSize;
    }

    /**
     * @return the digests
     */
    public Collection<URI> getDigests() {
        return digests;
    }

    /**
     * @return the externalHandling
     */
    public String getExternalHandling() {
        return externalHandling;
    }

    /**
     * @return the createdDate
     */
    public Instant getCreatedDate() {
        return createdDate;
    }

    /**
     * @return the createdBy
     */
    public String getCreatedBy() {
        return createdBy;
    }

    /**
     * @return the lastModifiedDate
     */
    public Instant getLastModifiedDate() {
        return lastModifiedDate;
    }

    /**
     * @return the lastModifiedBy
     */
    public String getLastModifiedBy() {
        return lastModifiedBy;
    }

    /**
     * @return the timestamp when the memento was created
     */
    public Instant getMementoCreatedDate() {
        return mementoCreatedDate;
    }

    /**
     * @return the externalUrl
     */
    public String getExternalUrl() {
        return externalUrl;
    }

    /**
     * @return if it is an archival group
     */
    public boolean isArchivalGroup() {
        return archivalGroup;
    }

    /**
     * @return if it is an object root resource
     */
    public boolean isObjectRoot() {
        if (isArchivalGroup()) {
            return true;
        } else {
            return objectRoot;
        }
    }

    /**
     * @return if the resource is deleted
     */
    public boolean isDeleted() {
        return deleted;
    }

    /**
     * @return the path to the associated content file
     */
    public String getContentPath() {
        return contentPath;
    }

    /**
     * @return the version identifier of the headers file
     */
    public String getHeadersVersion() {
        return headersVersion;
    }

    @Override
    public String toString() {
        return "ResourceHeaders{" +
                "id='" + id + '\'' +
                ", parent='" + parent + '\'' +
                ", archivalGroupId='" + archivalGroupId + '\'' +
                ", stateToken='" + stateToken + '\'' +
                ", interactionModel='" + interactionModel + '\'' +
                ", mimeType='" + mimeType + '\'' +
                ", filename='" + filename + '\'' +
                ", contentSize=" + contentSize +
                ", digests=" + digests +
                ", externalUrl='" + externalUrl + '\'' +
                ", externalHandling='" + externalHandling + '\'' +
                ", createdDate=" + createdDate +
                ", createdBy='" + createdBy + '\'' +
                ", lastModifiedDate=" + lastModifiedDate +
                ", lastModifiedBy='" + lastModifiedBy + '\'' +
                ", mementoCreatedDate=" + mementoCreatedDate +
                ", archivalGroup=" + archivalGroup +
                ", objectRoot=" + objectRoot +
                ", deleted=" + deleted +
                ", contentPath='" + contentPath + '\'' +
                ", headersVersion='" + headersVersion + '\'' +
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
        final ResourceHeaders that = (ResourceHeaders) o;
        return archivalGroup == that.archivalGroup &&
                objectRoot == that.objectRoot &&
                deleted == that.deleted &&
                Objects.equals(id, that.id) &&
                Objects.equals(parent, that.parent) &&
                Objects.equals(archivalGroupId, that.archivalGroupId) &&
                Objects.equals(stateToken, that.stateToken) &&
                Objects.equals(interactionModel, that.interactionModel) &&
                Objects.equals(mimeType, that.mimeType) &&
                Objects.equals(filename, that.filename) &&
                Objects.equals(contentSize, that.contentSize) &&
                Objects.equals(digests, that.digests) &&
                Objects.equals(externalUrl, that.externalUrl) &&
                Objects.equals(externalHandling, that.externalHandling) &&
                Objects.equals(createdDate, that.createdDate) &&
                Objects.equals(createdBy, that.createdBy) &&
                Objects.equals(lastModifiedDate, that.lastModifiedDate) &&
                Objects.equals(lastModifiedBy, that.lastModifiedBy) &&
                Objects.equals(mementoCreatedDate, that.mementoCreatedDate) &&
                Objects.equals(contentPath, that.contentPath) &&
                Objects.equals(headersVersion, that.headersVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, parent, archivalGroupId, stateToken,
                interactionModel, mimeType, filename,
                contentSize, digests, externalUrl,
                externalHandling, createdDate, createdBy,
                lastModifiedDate, lastModifiedBy, mementoCreatedDate,
                archivalGroup, objectRoot, deleted, contentPath, headersVersion);
    }

    /**
     * Builder for creating/mutating ResourceHeaders
     */
    @JsonPOJOBuilder
    public static class Builder {

        private String id;
        private String parent;
        private String archivalGroupId;
        private String stateToken;
        private String interactionModel;
        private String mimeType;
        private String filename;
        private long contentSize = -1;
        private Collection<URI> digests;
        private String externalUrl;
        private String externalHandling;
        private Instant createdDate;
        private String createdBy;
        private Instant lastModifiedDate;
        private String lastModifiedBy;
        private Instant mementoCreatedDate;
        private boolean archivalGroup;
        private boolean objectRoot;
        private boolean deleted;
        private String contentPath;
        private String headersVersion;

        public Builder() {

        }

        public Builder(final ResourceHeaders original) {
            this.id = original.getId();
            this.parent = original.getParent();
            this.archivalGroupId = original.getArchivalGroupId();
            this.stateToken = original.getStateToken();
            this.interactionModel = original.getInteractionModel();
            this.mimeType = original.getMimeType();
            this.filename = original.getFilename();
            this.contentSize = original.getContentSize();
            this.digests = new ArrayList<>(original.getDigests());
            this.externalUrl = original.getExternalUrl();
            this.externalHandling = original.getExternalHandling();
            this.createdDate = original.getCreatedDate();
            this.createdBy = original.getCreatedBy();
            this.lastModifiedDate = original.getLastModifiedDate();
            this.lastModifiedBy = original.getLastModifiedBy();
            this.mementoCreatedDate = original.getMementoCreatedDate();
            this.archivalGroup = original.isArchivalGroup();
            this.objectRoot = original.isObjectRoot();
            this.deleted = original.isDeleted();
            this.contentPath = original.getContentPath();
            this.headersVersion = original.getHeadersVersion();
        }

        public Builder withId(final String id) {
            this.id = id;
            return this;
        }

        public Builder withParent(final String parent) {
            this.parent = parent;
            return this;
        }

        public Builder withArchivalGroupId(final String archivalGroupId) {
            this.archivalGroupId = archivalGroupId;
            return this;
        }

        public Builder withStateToken(final String stateToken) {
            this.stateToken = stateToken;
            return this;
        }

        public Builder withInteractionModel(final String interactionModel) {
            this.interactionModel = interactionModel;
            return this;
        }

        public Builder withMimeType(final String mimeType) {
            this.mimeType = mimeType;
            return this;
        }

        public Builder withFilename(final String filename) {
            this.filename = filename;
            return this;
        }

        public Builder withContentSize(final long contentSize) {
            this.contentSize = contentSize;
            return this;
        }

        public Builder withDigests(final Collection<URI> digests) {
            this.digests = digests;
            return this;
        }

        public Builder addDigest(final URI digest) {
            if (digests == null) {
                digests = new ArrayList<>();
            }
            digests.add(digest);
            return this;
        }

        public Builder withExternalUrl(final String externalUrl) {
            this.externalUrl = externalUrl;
            return this;
        }

        public Builder withExternalHandling(final String externalHandling) {
            this.externalHandling = externalHandling;
            return this;
        }

        public Builder withCreatedDate(final Instant createdDate) {
            this.createdDate = createdDate;
            return this;
        }

        public Builder withCreatedBy(final String createdBy) {
            this.createdBy = createdBy;
            return this;
        }

        public Builder withLastModifiedDate(final Instant lastModifiedDate) {
            this.lastModifiedDate = lastModifiedDate;
            return this;
        }

        public Builder withLastModifiedBy(final String lastModifiedBy) {
            this.lastModifiedBy = lastModifiedBy;
            return this;
        }

        public Builder withMementoCreatedDate(final Instant mementoCreatedDate) {
            this.mementoCreatedDate = mementoCreatedDate;
            return this;
        }

        public Builder withArchivalGroup(final boolean archivalGroup) {
            this.archivalGroup = archivalGroup;
            return this;
        }

        public Builder withObjectRoot(final boolean objectRoot) {
            this.objectRoot = objectRoot;
            return this;
        }

        public Builder withDeleted(final boolean deleted) {
            this.deleted = deleted;
            return this;
        }

        public Builder withContentPath(final String contentPath) {
            this.contentPath = contentPath;
            return this;
        }

        public Builder withHeadersVersion(final String headersVersion) {
            this.headersVersion = headersVersion;
            return this;
        }

        public ResourceHeaders build() {
            return new ResourceHeaders(this);
        }
    }

}