/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
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
