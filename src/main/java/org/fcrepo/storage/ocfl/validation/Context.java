/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl.validation;

import org.fcrepo.storage.ocfl.exception.ValidationException;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Validation context for collecting problems
 *
 * @author pwinckles
 */
class Context {

    private final String ocflObjectId;
    private final Collection<String> problems;

    public Context() {
        this(null);
    }

    public Context(final String ocflObjectId) {
        this.ocflObjectId = ocflObjectId;
        this.problems = new ArrayList<>();
    }

    /**
     * Adds a new problem
     *
     * @param problem the problem message
     */
    public void problem(final String problem) {
        problems.add(problem);
    }

    /**
     * Adds a new problem
     *
     * @param format Java String.format() style message format
     * @param args the values to be substituted into the format
     */
    public void problem(final String format, final Object... args) {
        problems.add(String.format(format, args));
    }

    /**
     * @return the list of all collected problems
     */
    public Collection<String> getProblems() {
        return problems;
    }

    /**
     * Throws a ValidationException if there are problems
     */
    public void throwValidationException() {
        if (!problems.isEmpty()) {
            throw ValidationException.createForObject(ocflObjectId, problems);
        }
    }

}
