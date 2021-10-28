/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
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
