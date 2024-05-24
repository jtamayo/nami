package edu.stanford.nami;

import com.google.protobuf.ByteString;

/** Stores recent reads/writes that were performed on remote shards */
public class CachingStore {
  private static final int LRU_CAPACITY = 1000;

  LRUCache<NKey, ByteString> lruCache = new LRUCache<>(LRU_CAPACITY);

  public CachingStore() {}

  public ByteString get(NKey key) {
    return lruCache.get(key);
  }

  public void invalidate(NKey key) {
    this.lruCache.remove(key);
  }

  public void put(NKey key, ByteString value) {
    this.lruCache.put(key, value);
  }
}
