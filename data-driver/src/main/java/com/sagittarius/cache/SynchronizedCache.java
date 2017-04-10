package com.sagittarius.cache;

/**
 * Wrapper for caches that adds simple synchronization to provide a thread-safe cache.
 */
public class SynchronizedCache<K, V> implements Cache<K, V> {
    private final Cache<K, V> underlying;

    public SynchronizedCache(Cache<K, V> underlying) {
        this.underlying = underlying;
    }

    @Override
    public synchronized V get(K key) {
        return underlying.get(key);
    }

    @Override
    public synchronized void put(K key, V value) {
        underlying.put(key, value);
    }

    @Override
    public synchronized boolean remove(K key) {
        return underlying.remove(key);
    }

    @Override
    public synchronized long size() {
        return underlying.size();
    }
}
