package edu.stanford.nami;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.util.Objects;
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

  private boolean isInTransactionGetValueValid(
      long previousTransactionTid, InTransactionGet inTransactionGet) {
    System.out.println(
        "Validating inTxGet for previousTransactionTid "
            + previousTransactionTid
            + " and "
            + inTransactionGet);
    NKey nKey = new NKey(inTransactionGet.getKey());
    ByteString value;
    if (this.kvStore.hasKeyInAllocation(nKey)) {
      value =
          ByteString.copyFrom(
              withRocksDBRetries(() -> this.kvStore.getAsOf(nKey, previousTransactionTid)));
    } else {
      value = this.remoteStore.getAsOf(nKey, previousTransactionTid);
    }
    // TODO bring back caching store
    // if ((value = this.cachingStore.get(nvKey)) == null) {
    // NVKey nvKey = new NVKey(previousTransactionTid, inTransactionGet.getKey());
    // Retry here if fails?
    // ByteBuffer remoteValue = this.remoteStore.getAsOf(nKey,
    // previousTransactionTid).asReadOnlyByteBuffer();
    // Preconditions.checkState(
    //     remoteValue != null, "Remote fetched for a key that does not exist: " + nKey);
    // value = remoteValue.asReadOnlyBuffer().array();
    // // Also add this to the cachingStore?
    // this.cachingStore.put(nvKey, value);
    // value = null;
    // }
    Preconditions.checkState(value != null, "Read for a key that does not exist: " + nKey);
    return Objects.equals(value, inTransactionGet.getValue());
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
    // TODO this invalidate refers to the wrong tid (tid - 1 vs prior tid)
    // it might not matter if we simply store NKeys in the cache
    this.cachingStore.invalidate(currentTid - 1, nKey);
  }

  public TransactionStatus processTransaction(
      TransactionRequest request, long assignedTid, boolean isLeader) {
    System.out.println(
        "Processing transaction with assignedTid " + assignedTid + ", tx: " + request);
    TransactionStatus transactionStatus;
    // Change this if we ever decide to cache non-local transactions on followers
    if (isLeader || hasLocalWrites(request)) {
      // Leader constructs response to client, so it must always determine tx status
      // If you have local writes, then you also need to determine status
      var priorTid = kvStore.getLatestTid(); // check against most recently seen tid
      boolean foundInvalidGets = false;
      for (InTransactionGet inTransactionGet : request.getGetsList()) {
        if (!this.isInTransactionGetValueValid(priorTid, inTransactionGet)) {
          foundInvalidGets = true;
          break;
        }
      }
      if (foundInvalidGets) {
        transactionStatus = TransactionStatus.CONFLICT_ABORTED;
      } else {
        transactionStatus = TransactionStatus.COMMITTED;
      }
    } else {
      // no need to determine transaction status
      transactionStatus = TransactionStatus.UNKNOWN;
    }

    System.out.println("Determined tid " + assignedTid + " has status " + transactionStatus);

    // process transactions for cache
    switch (transactionStatus) {
      case UNKNOWN:
        // Invalidate the remote writes in local cache
        for (InTransactionPut inTransactionPut : request.getPutsList()) {
          this.invalidateTransactionPut(assignedTid, inTransactionPut);
        }
        break;
      default:
        // TODO process puts for other transaction statuses
        break;
    }

    // process transactions for local store
    switch (transactionStatus) {
      case COMMITTED:
        for (InTransactionPut inTransactionPut : request.getPutsList()) {
          this.processInTransactionPut(assignedTid, inTransactionPut);
        }
        break;
      case CONFLICT_ABORTED:
        // no-op
        break;
      case UNKNOWN:
        // sanity check: we should have no local writes
        Preconditions.checkState(!hasLocalWrites(request));
        break;
      default:
        throw new IllegalArgumentException("Unknown transaction status " + transactionStatus);
    }
    // regardless, we must update latest tid
    kvStore.updateLatestTid(assignedTid);

    return transactionStatus;
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
