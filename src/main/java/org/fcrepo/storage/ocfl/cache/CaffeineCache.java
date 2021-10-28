/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl.cache;

import java.util.Objects;
import java.util.function.Function;

/**
 * In-memory cache implementation that is a wrapper around a Caffeine cache.
 *
 * @author pwinckles
 */
public class CaffeineCache<K, V> implements Cache<K, V> {

    private com.github.benmanes.caffeine.cache.Cache<K, V> cache;

    public CaffeineCache(final com.github.benmanes.caffeine.cache.Cache cache) {
        this.cache = Objects.requireNonNull(cache, "cache cannot be null");
    }

    @Override
    public V get(final K key, final Function<K, V> loader) {
        return cache.get(key, loader);
    }

    @Override
    public void put(final K key, final V value) {
        cache.put(key, value);
    }

    @Override
    public void invalidate(final K key) {
        cache.invalidate(key);
    }

    @Override
    public void invalidateAll(final Iterable<K> keys) {
        cache.invalidateAll(keys);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }
}
