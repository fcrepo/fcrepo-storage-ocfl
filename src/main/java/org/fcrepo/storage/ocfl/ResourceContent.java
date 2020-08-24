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

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;

/**
 * Encapsulates a resource's content and its headers.
 *
 * @author pwinckles
 */
public class ResourceContent implements AutoCloseable {

    private final ResourceHeaders headers;
    private final Optional<InputStream> contentStream;

    /**
     * Creates a new instance
     *
     * @param contentStream the resource's content, may be null
     * @param headers the resource's headers
     */
    public ResourceContent(final InputStream contentStream, final ResourceHeaders headers) {
        this(Optional.ofNullable(contentStream), headers);
    }

    /**
     * Creates a new instance
     *
     * @param contentStream the resource's content
     * @param headers the resource's headers
     */
    public ResourceContent(final Optional<InputStream> contentStream, final ResourceHeaders headers) {
        this.contentStream = Objects.requireNonNull(contentStream, "contentStream cannot be null");
        this.headers = Objects.requireNonNull(headers, "headers cannot be null");
    }

    /**
     * @return the resource's content
     */
    public Optional<InputStream> getContentStream() {
        return contentStream;
    }

    /**
     * @return the resource's headers
     */
    public ResourceHeaders getHeaders() {
        return headers;
    }

    /**
     * Closes the underlying resource content stream.
     *
     * @throws IOException if the stream is not closed cleanly
     */
    @Override
    public void close() throws IOException {
        if (contentStream.isPresent()) {
            contentStream.get().close();
        }
    }

}
