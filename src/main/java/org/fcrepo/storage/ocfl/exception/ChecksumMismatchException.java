/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl.exception;

import java.util.List;
import java.util.Objects;

/**
 * Indicates that the actual checksum of a file did not match the expectation
 *
 * @author pwinckles
 */
public class ChecksumMismatchException extends RuntimeException {

    private final List<String> problems;

    private String message;

    public ChecksumMismatchException(final List<String> problems) {
        this.problems = Objects.requireNonNull(problems, "problems cannot be null");
    }

    @Override
    public String getMessage() {
        if (message == null) {
            final var builder = new StringBuilder("The following checksums did not match: ");

            var index = 1;

            for (var problem : problems) {
                builder.append("\n  ").append(index++).append(". ");
                builder.append(problem);
            }

            message = builder.toString();
        }

        return message;
    }

    public List<String> getProblems() {
        return problems;
    }

}
