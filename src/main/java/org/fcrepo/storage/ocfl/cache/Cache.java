/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.storage.ocfl.cache;

import java.util.function.Function;

/**
 * Cache interface
 *
 * @param <K> type of cache key
 * @param <V> type of cache value
 *
 * @author pwinckles
 */
public interface Cache<K,V> {

    /**
     * Retrieves a value from the cache. If it doesn't exist, the loader is called to load the object, which is then
     * cached and returned.
     *
     * @param key to lookup in the cache
     * @param loader function to call to load the object if it's not found
     * @return the object that maps to the key
     */
    V get(final K key, final Function<K, V> loader);

    /**
     * Inserts a value into the cache.
     *
     * @param key key
     * @param value value
     */
    void put(final K key, final V value);

    /**
     * Invalidates the key in the cache.
     *
     * @param key key
     */
    void invalidate(final K key);

    /**
     * Invalidates the keys provided in the cache.
     *
     * @param keys the keys.
     */
    void invalidateAll(final Iterable<K> keys);

    /**
     * Invalidate the entire cache.
     */
    void invalidateAll();
}
