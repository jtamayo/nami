package edu.stanford.nami;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
  private final int capacity;

  public LRUCache(int capacity) {
    super(capacity);
    this.capacity = capacity;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    return size() > this.capacity;
  }
}
