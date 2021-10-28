/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl.exception;

/**
 * Indicates that the content provided for a resource was invalid. For example, the expected and actual content sizes
 * did not match
 *
 * @author pwinckles
 */
public class InvalidContentException extends RuntimeException {

    public InvalidContentException() {
    }

    public InvalidContentException(final String message) {
        super(message);
    }

    public InvalidContentException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public InvalidContentException(final Throwable cause) {
        super(cause);
    }

}
