package edu.stanford.nami;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.extern.flogger.Flogger;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

@Flogger
public class TransactionProcessor {
  @Getter private final VersionedKVStore kvStore;
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

  private boolean areInTransactionGetValuesValid(
      long previousTransactionTid, List<InTransactionGet> inTransactionGets) {
    log.atFine().log(
        "Validating inTxGets %s for previousTransactionTid %s",
        inTransactionGets, previousTransactionTid);

    // first try to resolve all values locally
    var remoteInTxGets = new ArrayList<InTransactionGet>();
    for (var inTransactionGet : inTransactionGets) {
      NKey nKey = new NKey(inTransactionGet.getKey());
      ByteString value;
      if (this.kvStore.hasKeyInAllocation(nKey)) {
        value =
            ByteString.copyFrom(
                withRocksDBRetries(() -> this.kvStore.getAsOf(nKey, previousTransactionTid)));
      } else if ((value = this.cachingStore.get(nKey)) == null) {
        // we don't have this tx locally, so add it to remoteGets and move on
        remoteInTxGets.add(inTransactionGet);
        // note we must continue the loop, otherwise the comparison below will fail
        continue;
      }
      Preconditions.checkState(value != null, "Read for a key that does not exist: " + nKey);
      // if at least one value changed, we know for a fact the whole tx will not commit
      if (!Objects.equals(value, inTransactionGet.getValue())) {
        log.atFine().log("Transaction won't commit because key %s has changed", nKey);
        return false;
      }
    }

    // if there are remote values pending, then get those
    if (!remoteInTxGets.isEmpty()) {
      var remoteNKeys =
          remoteInTxGets.stream()
              .map(r -> new NKey(r.getKey()))
              .collect(ImmutableSet.toImmutableSet());
      var remoteValues = this.remoteStore.getAsOf(remoteNKeys, previousTransactionTid);
      // first update caching store with _all_ the remote values we just got
      this.cachingStore.putAll(previousTransactionTid, remoteValues);

      // then check tx status
      for (var remoteInTxGet : remoteInTxGets) {
        var nKey = new NKey(remoteInTxGet.getKey());
        var remoteValue = remoteValues.get(nKey);
        Preconditions.checkState(
            remoteValues.containsKey(nKey), "Remote values is missing key " + nKey);
        if (!Objects.equals(remoteValue, remoteInTxGet.getValue())) {
          log.atFine().log("Transaction won't commit because key %s has changed", nKey);
          return false;
        }
      }
    }
    // if all local and remote values are OK, then the tx will commit
    return true;
  }

  private void processInTransactionPuts(long currentTid, List<InTransactionPut> inTransactionPuts) {
    var relevantPuts = new ArrayList<InTransactionPut>();
    var irrelevantPuts = new ArrayList<InTransactionPut>();
    for (var put : inTransactionPuts) {
      NVKey nvKey = new NVKey(currentTid, put.getKey());
      if (this.kvStore.hasKeyInAllocation(nvKey.nKey())) {
        relevantPuts.add(put);
      } else {
        irrelevantPuts.add(put);
      }
    }
    if (!relevantPuts.isEmpty()) {
      withRocksDBRetries(
          () -> {
            kvStore.putInBatch(
                operation -> {
                  for (var put : relevantPuts) {
                    NVKey nvKey = new NVKey(currentTid, put.getKey());
                    var value = put.getValue();
                    operation.put(nvKey, value.toByteArray());
                  }
                });
            return null;
          });
    }

    for (var put : irrelevantPuts) {
      NVKey nvKey = new NVKey(currentTid, put.getKey());
      this.cachingStore.put(currentTid, nvKey.nKey(), put.getValue());
    }
  }

  private void invalidateTransactionPut(long currentTid, InTransactionPut inTransactionPut) {
    NKey nKey = new NKey(inTransactionPut.getKey());
    this.cachingStore.invalidate(nKey);
  }

  public TransactionStatus processTransaction(
      TransactionRequest request, long assignedTid, boolean isLeader) {
    log.atFine().log("Processing transaction with assignedTid " + assignedTid + ", tx: " + request);
    TransactionStatus transactionStatus;
    // Change this if we ever decide to cache non-local transactions on followers
    if (isLeader || hasLocalWrites(request)) {
      // Leader constructs response to client, so it must always determine tx status
      // If you have local writes, then you also need to determine status
      var priorTid = kvStore.getLatestTid(); // check against most recently seen tid
      boolean foundInvalidGets =
          !this.areInTransactionGetValuesValid(priorTid, request.getGetsList());
      if (foundInvalidGets) {
        transactionStatus = TransactionStatus.CONFLICT_ABORTED;
      } else {
        transactionStatus = TransactionStatus.COMMITTED;
      }
    } else {
      // no need to determine transaction status
      transactionStatus = TransactionStatus.UNKNOWN;
    }

    log.atFine().log("Determined tid " + assignedTid + " has status " + transactionStatus);

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
        this.processInTransactionPuts(assignedTid, request.getPutsList());
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
          log.atInfo().log("Retrying operation: " + numRetries + " time");
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
