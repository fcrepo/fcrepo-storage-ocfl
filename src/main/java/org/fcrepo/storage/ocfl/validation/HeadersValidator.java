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

import org.fcrepo.storage.ocfl.PersistencePaths;
import org.fcrepo.storage.ocfl.ResourceHeaders;
import org.fcrepo.storage.ocfl.exception.ValidationException;

/**
 * Interface for validating resource headers
 *
 * @author pwinckles
 */
public interface HeadersValidator {

    /**
     * Validates resource headers. The root headers MUST have a valid id and this method MUST NOT
     * be called without first validating it.
     *
     * @param paths the persistence paths for the resource, may be null
     * @param headers the headers to validate, may not be null
     * @param rootHeaders the headers for the resource at the root of the OCFL object, may not be null
     * @throws ValidationException when problems are identified
     */
    void validate(final PersistencePaths paths,
                  final ResourceHeaders headers,
                  final ResourceHeaders rootHeaders);

}
