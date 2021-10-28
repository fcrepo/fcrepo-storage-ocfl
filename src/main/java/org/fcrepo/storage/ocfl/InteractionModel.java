/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl;

/**
 * Represents known interaction models.
 *
 * @author pwinckles
 */
public enum InteractionModel {

    NON_RDF("http://www.w3.org/ns/ldp#NonRDFSource"),
    NON_RDF_DESCRIPTION("http://fedora.info/definitions/v4/repository#NonRdfSourceDescription"),
    ACL("http://fedora.info/definitions/v4/webac#Acl"),
    BASIC_CONTAINER("http://www.w3.org/ns/ldp#BasicContainer"),
    INDIRECT_CONTAINER("http://www.w3.org/ns/ldp#IndirectContainer"),
    DIRECT_CONTAINER("http://www.w3.org/ns/ldp#DirectContainer");

    private final String uri;

    InteractionModel(final String uri) {
        this.uri = uri;
    }

    /**
     * @return the uri of the interaction model
     */
    public String getUri() {
        return uri;
    }

    /**
     * @param value interaction model uri
     * @return the model mapped to the uri
     * @throws IllegalArgumentException if there is no model mapped to the uri
     */
    public static InteractionModel fromString(final String value) {
        for (var model : values()) {
            if (model.uri.equals(value)) {
                return model;
            }
        }
        throw new IllegalArgumentException("Unknown interaction model: " + value);
    }

}
