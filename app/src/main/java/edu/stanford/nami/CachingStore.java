package edu.stanford.nami;

/** Stores recent reads/writes that were performed on remote shards */
public class CachingStore {
  private static final int LRU_CAPACITY = 1000;

  LRUCache<NVKey, byte[]> lruCache = new LRUCache<>(LRU_CAPACITY);

  public CachingStore() {}

  public byte[] get(NVKey key) {
    // return lruCache.get(key);
    return null;
  }

  public void invalidate(long tid, NKey key) {
    // TODO Need to remove all keys as of tid and prior
    //    this.lruCache.remove(key);
  }

  public void put(NVKey key, byte[] value) {
    // TODO cache should only store key, not version
    //    this.lruCache.put(key, value);
  }
}
