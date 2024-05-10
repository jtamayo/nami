package edu.stanford.nami;

import com.google.common.base.Preconditions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * A versioned key-value store backed by a RocksDB database. Every key is versioned by a tid, which
 * refers to the transaction that wrote it.
 *
 * <p>For a given key, clients can query either the latest value or the value as of a specific tid.
 * In that case, we will return the value in the most recent transaction up to and including that
 * tid.
 *
 * <p>Every write must include the tid that's writing it.
 *
 * <p>Known limitations: - No deletes. We'd have to implement them with tombstones, prob with empty
 * byte[]? - How do I implement nulls? is that even allowed?
 *
 * <p>
 */
public class VersionedKVStore {
  // TODO: Does this need to be aware of partitions at all? 
  // TODO: do we want to check that the tid that's writing is after the most recent tid? 
  // TODO: how do we implement transactions? do we need to at all?
  // TODO: do I need to store tid as part of every transaction? I need every write to be atomic,
  // and AFAICT, put doesn't necessarily flush to disk. I also need to know that, if I crash,
  // I'll know where to restart.

  private final RocksDB db;

  public VersionedKVStore(RocksDB db) {
    this.db = db;
  }

  public void put(NVKey key, byte[] value) throws RocksDBException {
    db.put(key.toBytes(), value);
  }

  public byte[] get(NVKey key) throws RocksDBException {
    return db.get(key.toBytes());
  }
}
