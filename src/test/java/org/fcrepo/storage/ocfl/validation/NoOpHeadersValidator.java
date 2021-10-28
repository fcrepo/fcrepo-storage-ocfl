/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl.validation;

import org.fcrepo.storage.ocfl.PersistencePaths;
import org.fcrepo.storage.ocfl.ResourceHeaders;

/**
 * A headers validator that does nothing. This is useful when wanting to intentionally create invalid objects for
 * testing purposes.
 *
 * @author pwinckles
 */
public class NoOpHeadersValidator implements HeadersValidator {
    @Override
    public void validate(final PersistencePaths paths,
                         final ResourceHeaders headers,
                         final ResourceHeaders rootHeaders) {
        // no-op
    }
}
