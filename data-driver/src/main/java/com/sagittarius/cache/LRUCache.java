package com.sagittarius.cache;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A cache implementing a least recently used policy.
 */
public class LRUCache<K, V> implements Cache<K, V> {
    private final LinkedHashMap<K, V> cache;

    public LinkedHashMap<K, V> getCache() {
        return cache;
    }

    public LRUCache(final int maxSize) {
        cache = new LinkedHashMap<K, V>(16, .75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > maxSize;
            }
        };
    }

    @Override
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public boolean remove(K key) {
        return cache.remove(key) != null;
    }

    @Override
    public long size() {
        return cache.size();
    }
}
