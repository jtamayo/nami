package edu.stanford.nami.nativerocksdb;

import com.google.protobuf.ByteString;
import edu.stanford.nami.*;
import edu.stanford.nami.client.ClientTransaction;
import lombok.extern.flogger.Flogger;

@Flogger
public class NativeRocksDBClientTransaction implements ClientTransaction {
  private final long tid;
  private final NativeRocksDBClient nativeRocksDBClient;

  private NativeRocksDBClientTransaction(NativeRocksDBClient nativeRocksDBClient, long tid) {
    this.tid = tid;
    this.nativeRocksDBClient = nativeRocksDBClient;
  }

  public ByteString get(NKey key) {
    NativeRocksInTransactionGetForUpdateResponse response =
        nativeRocksDBClient.getForUpdate(tid, key.key());
    return response.getValue();
  }

  public void put(NKey key, ByteString value) {
    nativeRocksDBClient.put(tid, key.key(), value);
  }

  // Attempt to commit this transaction to the server
  public TransactionResponse commit() {
    NativeRocksTransactionCommitResponse response = nativeRocksDBClient.commit(tid);
    TransactionResponse.Builder builder = TransactionResponse.newBuilder();
    NativeRocksTransactionStatus status = response.getStatus();
    TransactionStatus namiStatus;
    switch (status) {
      case COMMITTED -> namiStatus = TransactionStatus.COMMITTED;
      case ABORTED -> namiStatus = TransactionStatus.CONFLICT_ABORTED;
      default -> namiStatus = TransactionStatus.UNKNOWN;
    }
    builder.setStatus(namiStatus);
    return builder.build();
  }

  // start a new transaction against the provided Nami cluster
  public static NativeRocksDBClientTransaction begin(NativeRocksDBClient client) {
    var response = client.begin();
    var tid = response.getTid();
    return new NativeRocksDBClientTransaction(client, tid);
  }
}
