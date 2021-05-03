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
    public void invalidateAll(final Iterable<? extends K> keys) {
        cache.invalidateAll(keys);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }
}
