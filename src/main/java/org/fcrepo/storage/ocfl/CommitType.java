/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl;

/**
 * Options for defining the behavior when performing a commit to the persistent storage layer.
 *
 * @author bbpennel
 */
public enum CommitType {
    /* Commit unversioned content */
    UNVERSIONED,
    /* Commit a new version */
    NEW_VERSION
}
