package idb.utils;

import java.util.LinkedHashMap;
import java.util.Map;

public class Cache<K, V> {
    private final int maxSize;
    private final LinkedHashMap<K, V> cacheMap;

    public Cache(final int maxSize) {
        this.maxSize = maxSize;
        this.cacheMap = new LinkedHashMap<K, V>(maxSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > maxSize;
            }
        };
    }

    public synchronized V get(K key) {
        return cacheMap.get(key);
    }

    public synchronized void put(K key, V value) {
        cacheMap.put(key, value);
    }

    public synchronized void remove(K key) {
        cacheMap.remove(key);
    }

    public synchronized void clear() {
        cacheMap.clear();
    }
}

