/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl.exception;

import java.util.Collection;
import java.util.Objects;

/**
 * Indicates that an OCFL object is not a valid Fedora 6 object.
 *
 * @author pwinckles
 */
public class ValidationException extends RuntimeException {

    private final String resourceId;
    private final String ocflObjectId;
    private final Collection<String> problems;

    private String message;

    /**
     * @param problems the validation problems
     * @return validation exception
     */
    public static ValidationException create(final Collection<String> problems) {
        return new ValidationException(null, null, problems);
    }

    /**
     * @param resourceId the Fedora resource id that is invalid
     * @param problems the validation problems
     * @return validation exception
     */
    public static ValidationException createForResource(final String resourceId, final Collection<String> problems) {
        return new ValidationException(null, resourceId, problems);
    }

    /**
     * @param ocflObjectId the ocfl object id that is invalid
     * @param problems the validation problems
     * @return validation exception
     */
    public static ValidationException createForObject(final String ocflObjectId, final Collection<String> problems) {
        return new ValidationException(ocflObjectId, null, problems);
    }

    private ValidationException(final String ocflObjectId,
                               final String resourceId,
                               final Collection<String> problems) {
        this.ocflObjectId = ocflObjectId;
        this.resourceId = resourceId;
        this.problems = Objects.requireNonNull(problems, "problems cannot be null");
    }

    @Override
    public String getMessage() {
        if (message == null) {
            final var builder = new StringBuilder();

            if (ocflObjectId != null) {
                builder.append("OCFL object ").append(ocflObjectId).append(" is not a valid Fedora 6 object. ");
            }

            if (resourceId != null) {
                builder.append("Resource ").append(resourceId).append(" is not a valid Fedora 6 resource. ");
            }

            builder.append("The following problems were identified:");

            var index = 1;

            for (var problem : problems) {
                builder.append("\n  ").append(index++).append(". ");
                builder.append(problem);
            }

            message = builder.toString();
        }

        return message;
    }

    public Collection<String> getProblems() {
        return problems;
    }

}
