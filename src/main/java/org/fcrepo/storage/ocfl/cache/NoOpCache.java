/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl.cache;

import java.util.function.Function;

/**
 * Pass-through cache implementation that never caches.
 *
 * @author pwinckles
 */
public class NoOpCache<K, V> implements Cache<K, V> {

    @Override
    public V get(final K key, final Function<K, V> loader) {
        return loader.apply(key);
    }

    @Override
    public void put(final K key, final V value) {
        // no op
    }

    @Override
    public void invalidate(final K key) {
        // no op
    }

    @Override
    public void invalidateAll(final Iterable<K> keys) {
        // no op
    }

    @Override
    public void invalidateAll() {
        // no op
    }
}
