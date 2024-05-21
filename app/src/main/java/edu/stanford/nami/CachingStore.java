package edu.stanford.nami;

/** Stores recent reads/writes that were performed on remote shards */
public class CachingStore {
  private static final int LRU_CAPACITY = 1000;

  LRUCache<NVKey, byte[]> lruCache = new LRUCache<>(LRU_CAPACITY);

  public CachingStore() {}

  public byte[] get(NVKey key) {
    //    return lruCache.get(key);
    return null;
  }

  public void invalidate(long tid, NKey key) {
    // Need to remove all keys as of tid and prior
    //    this.lruCache.remove(key);
  }

  public void put(NVKey key, byte[] value) {
    //    this.lruCache.put(key, value);
  }
}
