package edu.stanford.nami;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.protobuf.ByteString;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

public class TransactionProcessor {
  private final VersionedKVStore kvStore;
  private final RemoteStore remoteStore;
  private final CachingStore cachingStore;
  private static final int NUM_DB_RETRIES = 3;

  public TransactionProcessor(
      VersionedKVStore kvStore, RemoteStore remoteStore, CachingStore cachingStore) {
    this.kvStore = kvStore;
    this.remoteStore = remoteStore;
    this.cachingStore = cachingStore;
  }

  private boolean hasLocalWrites(TransactionRequest request) {
    for (InTransactionPut inTransactionPut : request.getPutsList()) {
      NKey key = new NKey(inTransactionPut.getKey());
      if (this.kvStore.hasKeyInAllocation(key)) {
        return true;
      }
    }
    return false;
  }

  private boolean isInTransactionGetValueValid(long tid, InTransactionGet inTransactionGet) {
    NKey nKey = new NKey(inTransactionGet.getKey());
    NVKey nvKey = new NVKey(tid, inTransactionGet.getKey());
    byte[] value;
    if (this.kvStore.hasKeyInAllocation(nKey)) {
      value = withRocksDBRetries(() -> this.kvStore.getAsOf(nKey, tid));
    } else if ((value = this.cachingStore.get(nvKey)) == null) {
      // Retry here if fails?
      ByteBuffer remoteValue = this.remoteStore.getAsOf(nKey, tid).asReadOnlyByteBuffer();
      Preconditions.checkState(
          remoteValue != null, "Remote fetched for a key that does not exist: " + nKey);
      value = ByteString.copyFrom(remoteValue).toByteArray();
      // Also add this to the cachingStore?
      this.cachingStore.put(nvKey, value);
    }
    Preconditions.checkState(value != null, "Read for a key that does not exist: " + nKey);
    return Arrays.equals(value, inTransactionGet.getValue().toByteArray());
  }

  private void processInTransactionPut(long currentTid, InTransactionPut inTransactionPut) {
    NVKey nvKey = new NVKey(currentTid, inTransactionPut.getKey());
    com.google.protobuf.ByteString value = inTransactionPut.getValue();
    if (this.kvStore.hasKeyInAllocation(new NKey(inTransactionPut.getKey()))) {
      withRocksDBRetries(
          () -> {
            this.kvStore.put(nvKey, value.toByteArray());
            return null;
          });
    } else {
      this.cachingStore.put(nvKey, value.toByteArray());
    }
  }

  private void invalidateTransactionPut(long currentTid, InTransactionPut inTransactionPut) {
    NKey nKey = new NKey(inTransactionPut.getKey());
    this.cachingStore.invalidate(currentTid - 1, nKey);
  }

  public TransactionStatus processTransaction(
      TransactionRequest request, long currentTid, boolean isLeader) {
    // Change this if we ever decide to cache non-local transactions on followers
    if (!isLeader && !hasLocalWrites(request)) {
      // Invalidate the remote writes in local cache
      for (InTransactionPut inTransactionPut : request.getPutsList()) {
        this.invalidateTransactionPut(currentTid, inTransactionPut);
      }
      return TransactionStatus.UNKNOWN;
    }
    for (InTransactionGet inTransactionGet : request.getGetsList()) {
      // TODO: special case for currentTid = 0?
      if (!this.isInTransactionGetValueValid(currentTid - 1, inTransactionGet)) {
        return TransactionStatus.CONFLICT_ABORTED;
      }
    }
    for (InTransactionPut inTransactionPut : request.getPutsList()) {
      this.processInTransactionPut(currentTid, inTransactionPut);
    }
    return TransactionStatus.COMMITTED;
  }

  private <T> T withRocksDBRetries(RocksDBOperation<T> operation) {
    int numRetries = 0;
    while (true) {
      try {
        return operation.execute();
      } catch (RocksDBException e) {
        // Fix this to capture transient errors vs un-retriable errors?
        if (numRetries <= NUM_DB_RETRIES
            && (e.getStatus().getCode() == Status.Code.Aborted
                || e.getStatus().getCode() == Status.Code.Expired
                || e.getStatus().getCode() == Status.Code.TimedOut)) {
          numRetries++;
          System.out.println("Retrying operation: " + numRetries + " time");
          e.printStackTrace();
          continue;
        }
        // Eventually give up, something's very broken
        throw new RuntimeException(e);
      }
    }
  }

  @FunctionalInterface
  public static interface RocksDBOperation<T> {
    T execute() throws RocksDBException;
  }
}
