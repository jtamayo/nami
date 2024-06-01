package edu.stanford.nami.nativerocksdb;

import edu.stanford.nami.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.flogger.Flogger;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

@RequiredArgsConstructor
@Flogger
public class NativeRocksStore {
  public static final long GLOBAL_VERSION = 1;

  private final OptimisticTransactionDB db;

  private final AtomicLong nextTid = new AtomicLong();

  private final ConcurrentHashMap<Long, Transaction> transactions = new ConcurrentHashMap<>();

  public long begin() {
    try (var options = new WriteOptions()) {
      options.setSync(true);
      var tid = nextTid.getAndIncrement();
      var transaction = db.beginTransaction(options);
      transactions.put(tid, transaction);
      return tid;
    }
  }

  public byte[] getForUpdate(long tid, NKey key) throws RocksDBException {
    return withTransaction(
        tid,
        transaction -> {
          try (var options = new ReadOptions()) {
            var versionedKey = new NVKey(GLOBAL_VERSION, key.key());
            return transaction.getForUpdate(options, versionedKey.toBytes(), false);
          }
        });
  }

  public void putInTransaction(long tid, NKey key, byte[] value) throws RocksDBException {
    withTransaction(
        tid,
        transaction -> {
          try (var options = new WriteOptions()) {
            var versionedKey = new NVKey(GLOBAL_VERSION, key.key());
            transaction.put(versionedKey.toBytes(), value);
          }
          return null; // useless return value, but whatever
        });
  }

  public boolean commit(long tid) throws RocksDBException {
    return withTransaction(
        tid,
        transaction -> {
          try {
            transaction.commit();
            // everything went well
            return true;
          } catch (RocksDBException e) {
            var statusCode = e.getStatus().getCode();
            if (statusCode.equals(Status.Code.Busy) || statusCode.equals(Status.Code.TryAgain)) {
              // conflict, tell client to try again
              return false;
            } else {
              // anything else treat as an error, but still propagate it up
              log.atSevere().log("Error while committing transaction %s", tid, e);
              throw e;
            }
          }
        });
  }

  private <T> T withTransaction(long tid, TransactionalRocksDBOperation<T> operation)
      throws RocksDBException {
    var transaction = findTransactionOrThrow(tid);
    return operation.execute(transaction);
  }

  private Transaction findTransactionOrThrow(long tid) {
    var transaction = transactions.get(tid);
    if (transaction == null) {
      log.atSevere().log("Transaction with tid %s does not exist", tid);
      throw new IllegalArgumentException("Transaction with tid " + tid + " does not exist");
    }
    return transaction;
  }

  @FunctionalInterface
  public static interface TransactionalRocksDBOperation<T> {
    T execute(Transaction transaction) throws RocksDBException;
  }
}
