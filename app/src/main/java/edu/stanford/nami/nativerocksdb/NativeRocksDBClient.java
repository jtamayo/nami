package edu.stanford.nami.nativerocksdb;

import com.codahale.metrics.Timer;
import com.google.protobuf.ByteString;
import edu.stanford.nami.*;
import edu.stanford.nami.client.ClientMetrics;
import io.grpc.Channel;
import lombok.experimental.UtilityClass;
import lombok.extern.flogger.Flogger;

@Flogger
public class NativeRocksDBClient {
  private final NativeRocksDBGrpc.NativeRocksDBBlockingStub blockingStub;

  public NativeRocksDBClient(Channel channel) {
    blockingStub = NativeRocksDBGrpc.newBlockingStub(channel);
  }

  @UtilityClass
  public static final class Timers {
    Timer begin;
    Timer getForUpdate;
    Timer put;
    Timer commit;

    public static void recreateTimers(String prefix) {
      begin = ClientMetrics.registry.timer(prefix + ".native-rocks-db-client.begin");
      getForUpdate = ClientMetrics.registry.timer(prefix + ".native-rocks-db-client.getForUpdate");
      put = ClientMetrics.registry.timer(prefix + ".native-rocks-db-client.put");
      commit = ClientMetrics.registry.timer(prefix + ".native-rocks-db.commit");
    }
  }

  public NativeRocksInTransactionPutResponse put(long tid, String key, ByteString value) {
    var timer = NativeRocksDBClient.Timers.put.time();
    try {
      NativeRocksInTransactionPutRequest request =
          NativeRocksInTransactionPutRequest.newBuilder()
              .setKey(key)
              .setValue(value)
              .setTid(tid)
              .build();
      NativeRocksInTransactionPutResponse response = blockingStub.putIntransaction(request);
      log.atFine().log("Got put response: %s", response.toString());
      return response;
    } finally {
      timer.stop();
    }
  }

  public NativeRocksTransactionCommitResponse commit(long tid) {
    var timer = NativeRocksDBClient.Timers.commit.time();
    try {
      var request = NativeRocksTransactionCommitRequest.newBuilder().setTid(tid).build();
      var response = blockingStub.commitTransaction(request);
      log.atFine().log("Got commit response: %s", response.toString());
      return response;
    } finally {
      timer.stop();
    }
  }

  public NativeRocksBeginTransactionResponse begin() {
    var timer = NativeRocksDBClient.Timers.begin.time();
    try {
      var request = NativeRocksBeginTransactionRequest.newBuilder().build();
      var response = blockingStub.beginTransaction(request);
      log.atFine().log("Got begin response: %s", response.toString());
      return response;
    } finally {
      timer.stop();
    }
  }

  public NativeRocksInTransactionGetForUpdateResponse getForUpdate(long tid, String key) {
    var timer = NativeRocksDBClient.Timers.getForUpdate.time();
    try {
      var request =
          NativeRocksInTransactionGetForUpdateRequest.newBuilder().setTid(tid).setKey(key).build();
      var response = blockingStub.getForUpdateInTransaction(request);
      log.atFine().log("Got getForUpdate response: %s", response.toString());
      return response;
    } finally {
      timer.stop();
    }
  }
}
