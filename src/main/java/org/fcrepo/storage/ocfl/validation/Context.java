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

package org.fcrepo.storage.ocfl.validation;

import org.fcrepo.storage.ocfl.exception.ValidationException;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Validation context for collecting problems
 *
 * @pwinckles
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
            throw new ValidationException(ocflObjectId, problems);
        }
    }

}
