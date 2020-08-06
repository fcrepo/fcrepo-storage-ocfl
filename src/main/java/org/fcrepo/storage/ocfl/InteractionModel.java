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
