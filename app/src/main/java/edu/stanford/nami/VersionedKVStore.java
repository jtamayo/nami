package edu.stanford.nami;

import com.google.common.base.Preconditions;
import lombok.extern.flogger.Flogger;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

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
@Flogger
public class VersionedKVStore {
  // TODO: Does this need to be aware of partitions at all?
  // TODO: do we want to check that the tid that's writing is after the most recent tid?
  // TODO: how do we implement transactions? do we need to at all?
  // TODO: do I need to store tid as part of every transaction? I need every write to be atomic,
  // and AFAICT, put doesn't necessarily flush to disk. I also need to know that, if I crash,
  // I'll know where to restart.
  private final Chunks.KeyToChunkMapper keyToChunkMapper = Chunks.NaiveKeyToChunkMapper.INSTANCE;

  private final RocksDB db;
  private final Chunks.PeerAllocation peerAllocation;
  private final TidSynchronizer tidSynchronizer = new TidSynchronizer();

  public VersionedKVStore(RocksDB db, Chunks.PeerAllocation peerAllocation) {
    this.db = db;
    this.peerAllocation = peerAllocation;
  }

  public void put(NVKey key, byte[] value) throws RocksDBException {
    log.atFine().log("Storing NVKey " + key);
    Preconditions.checkArgument(
        this.hasKeyInAllocation(key.nKey()), "tid is not in this store's allocation");
    db.put(key.toBytes(), value);
  }

  public void updateLatestTid(long newTid) {
    // TODO this is wrong: we'll update tid before all values have been updated
    // we need to move all puts to this store and apply them in a rocks transaction
    tidSynchronizer.updateLatestTid(newTid);
  }

  public boolean hasKeyInAllocation(NKey key) {
    var keyChunk = keyToChunkMapper.map(key);
    return peerAllocation.ranges().stream()
        .anyMatch(range -> range.min() <= keyChunk && keyChunk <= range.max());
  }

  public byte[] getExactlyAtVersion(NVKey key) throws RocksDBException {
    return db.get(key.toBytes());
  }

  public void flush() throws RocksDBException {
    // TODO: add benchmark metrics here to test
    db.flushWal(true);

    FlushOptions flushOptions = new FlushOptions();
    // Set if the flush operation shall block until it terminates.
    flushOptions.setWaitForFlush(true);
    // Set to false so that flush would not proceed immediately even it means writes will
    // stall for the duration of the flush.
    flushOptions.setAllowWriteStall(false);
    db.flush(flushOptions);
  }

  /** Get a value as of tid or before it. */
  public byte[] getAsOf(NKey key, long tid) throws RocksDBException {
    Preconditions.checkArgument(
        this.hasKeyInAllocation(key), "tid is not in this store's allocation");
    Preconditions.checkArgument(tid > 0, "tid must be non negative");
    // sanity check: make sure we're past the requested tid
    tidSynchronizer.checkHasSeenTid(tid);
    try (RocksIterator it = db.newIterator()) {
      // seek to last possible transaction
      // TODO: use prefix search to prune search space
      var end = new NVKey(tid, key.key());
      it.seekForPrev(end.toBytes());
      if (it.isValid()) {
        var readKey = NVKey.fromBytes(it.key());
        if (readKey.key().equals(key.key())) {
          // found it
          return it.value();
        } else {
          // key not found
          return null;
        }
      } else {
        // check whether shit broke or if the data wasn't found
        it.status();
        // looks like we're ok
        return null;
      }
    }
  }

  public byte[] getLatest(NKey key) throws RocksDBException {
    return getAsOf(key, Long.MAX_VALUE);
  }

  public void waitUtilTid(long tid, long timeoutMillis) throws InterruptedException {
    this.tidSynchronizer.waitUtilTid(tid, timeoutMillis);
  }

  public long getLatestTid() {
    // HACK probably should grab the lock to be safe, but volatile should be enough
    return this.tidSynchronizer.latestTid;
  }

  public synchronized void resetLatestTid(long tid) {
    this.tidSynchronizer.resetLatestTid(tid);
  }

  private static final class TidSynchronizer {
    private volatile long latestTid;

    public synchronized void updateLatestTid(long tid) {
      log.atFine().log("Trying to update latestTid to " + tid);
      if (this.latestTid < tid) {
        log.atFine().log("Updating latestTid to " + tid);
        this.latestTid = tid;
        this.notifyAll();
      }
    }

    public synchronized void resetLatestTid(long tid) {
      log.atFine().log("Trying to reset latestTid to " + tid);
      // TODO: make this atomic/add a lock???
      this.latestTid = tid;
      this.notifyAll();
    }

    public synchronized void checkHasSeenTid(long tid) {
      Preconditions.checkState(
          this.latestTid >= tid,
          "tid " + tid + " has not been processed, latest tid is " + latestTid);
    }

    /** Waits until this synchronizer has reached or passed the provided tid. */
    public synchronized void waitUtilTid(long tid, long timeoutMillis) throws InterruptedException {
      while (latestTid < tid) {
        log.atFine().log("Waiting to see tid " + tid + ", latestTid is " + latestTid);
        // TODO this is the wrong time to wait, I need to subtract the time I've waited already
        this.wait(timeoutMillis);
      }
    }
  }
}
