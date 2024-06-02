package edu.stanford.nami;

import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;

/** Stores recent reads/writes that were performed on remote shards */
public class CachingStore {
  private long oldest_tid;
  private long newest_tid;
  private static final int TRANSACTIONS_CAPACITY = 100;

  HashMap<Long, HashMap<NKey, ByteString>> cache = new HashMap<>(TRANSACTIONS_CAPACITY);
  // Map to the most recent tid that processed this key - no capacity limit?
  HashMap<NKey, Long> index = new HashMap<>();

  public CachingStore() {
    oldest_tid = Long.MAX_VALUE;
    newest_tid = 0L;
  }

  public ByteString get(NKey key) {
    Long tid = index.get(key);
    if (tid == null) {
      return null;
    }
    HashMap<NKey, ByteString> writtenValues = cache.get(tid);
    return writtenValues.get(key);
  }

  private void evictOldest() {
    while (!cache.containsKey(oldest_tid)) {
      oldest_tid++;
    }
    HashMap<NKey, ByteString> writtenValues = cache.get(oldest_tid);
    for (Map.Entry<NKey, ByteString> entry : writtenValues.entrySet()) {
      index.remove(entry.getKey());
    }

    cache.remove(oldest_tid);
    oldest_tid++;
  }

  private boolean shouldEvict() {
    return cache.size() == TRANSACTIONS_CAPACITY;
  }

  public void invalidate(NKey key) {
    if (index.containsKey(key)) {
      long tid = index.get(key);
      HashMap<NKey, ByteString> writtenValues = cache.get(tid);
      writtenValues.remove(key);
      // Need to re-put this?
      if (!writtenValues.isEmpty()) {
        cache.put(tid, writtenValues);
      } else {
        cache.remove(tid);
      }
      index.remove(key);
    }
  }

  public void putAll(long tid, Map<NKey, ByteString> values) {
    for (Map.Entry<NKey, ByteString> entry : values.entrySet()) {
      index.put(entry.getKey(), tid);
      if (!cache.containsKey(tid)) {
        cache.put(tid, new HashMap<>());
      }
      cache.get(tid).put(entry.getKey(), entry.getValue());
    }
    if (tid < oldest_tid) {
      oldest_tid = tid;
    }
    if (tid > newest_tid) {
      newest_tid = tid;
    }
    if (shouldEvict()) {
      evictOldest();
    }
  }

  public void put(long tid, NKey key, ByteString value) {
    index.put(key, tid);
    if (!cache.containsKey(tid)) {
      cache.put(tid, new HashMap<>());
    }
    cache.get(tid).put(key, value);
    if (tid < oldest_tid) {
      oldest_tid = tid;
    }
    if (tid > newest_tid) {
      newest_tid = tid;
    }
    if (shouldEvict()) {
      evictOldest();
    }
  }
}
