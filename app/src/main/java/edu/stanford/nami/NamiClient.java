package edu.stanford.nami;

import static edu.stanford.nami.ProtoUtils.convertToRatisByteString;

import com.codahale.metrics.Timer;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.stanford.nami.client.ClientMetrics;
import edu.stanford.nami.config.ChunksConfig;
import edu.stanford.nami.config.PeersConfig;
import edu.stanford.nami.utils.AutoCloseables;
import edu.stanford.nami.utils.GrpcRetries;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import lombok.experimental.UtilityClass;
import lombok.extern.flogger.Flogger;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;

@Flogger
public final class NamiClient implements AutoCloseable {
  @UtilityClass
  public static final class Timers {
    Timer remoteGet;
    Timer getRecentTid;
    Timer commit;

    public static void recreateTimers(String prefix) {
      remoteGet = ClientMetrics.registry.timer(prefix + ".nami-client.remoteGet");
      getRecentTid = ClientMetrics.registry.timer(prefix + ".nami-client.getRecentTid");
      commit = ClientMetrics.registry.timer(prefix + ".nami-client.commit");
    }
  }

  /**
   * Give each client a unique identifier, useful for debugging and telling the RemoteStore this
   * process has no data.
   */
  private final String clientId = UUID.randomUUID().toString();

  private final RaftClient raftClient;
  private final RemoteStore remoteStore;

  @Override
  public void close() throws Exception {
    log.atInfo().log("Closing NamiClient");
    AutoCloseables.closeSafely(raftClient, remoteStore);
  }

  // build the client
  static RaftClient newRaftClient(PeersConfig peersConfig) {
    return RaftClient.newBuilder()
        .setProperties(new RaftProperties())
        .setRaftGroup(peersConfig.getRaftGroup())
        .build();
  }

  public NamiClient(PeersConfig peersConfig, ChunksConfig chunksConfig) {
    raftClient = newRaftClient(peersConfig);
    remoteStore = new RemoteStore(clientId, peersConfig, chunksConfig);
  }

  public long getRecentTid() {
    var timer = Timers.getRecentTid.time();
    try {
      return GrpcRetries.withGrpcRetries(
          () -> {
            var request = GetRecentTidRequest.newBuilder().build();
            var response = remoteStore.getArbitraryPeer().getRecentTid(request);
            var recentTid = response.getTid();
            log.atFine().log("Recent TID: %s", recentTid);
            return response.getTid();
          });
    } finally {
      timer.stop();
    }
  }

  public ByteString get(long tid, String key) {
    var timer = Timers.remoteGet.time();
    try {
      return remoteStore.getAsOf(new NKey(key), tid);
    } finally {
      timer.stop();
    }
  }

  public TransactionResponse commit(
      long snapshotTid, Map<NKey, ByteString> readValues, Map<NKey, ByteString> writtenValues) {
    var timer = Timers.commit.time();
    try {
      var builder = TransactionRequest.newBuilder();
      builder.setSnapshotTid(snapshotTid);
      for (var readValue : readValues.entrySet()) {
        var inTransactionGet =
            InTransactionGet.newBuilder()
                .setKey(readValue.getKey().key())
                .setValue(readValue.getValue())
                .build();
        builder.addGets(inTransactionGet);
      }
      for (var writeValue : writtenValues.entrySet()) {
        var inTransactionPut =
            InTransactionPut.newBuilder()
                .setKey(writeValue.getKey().key())
                .setValue(writeValue.getValue())
                .build();
        builder.addPuts(inTransactionPut);
      }

      var raftRequest = KVStoreRaftRequest.newBuilder().setTransaction(builder).build();
      return submitRaftRequest(raftRequest).getTransaction();
    } finally {
      timer.stop();
    }
  }

  private KVStoreRaftResponse submitRaftRequest(KVStoreRaftRequest request) {
    var raftMessage = Message.valueOf(convertToRatisByteString(request.toByteString()));

    try {
      RaftClientReply reply = raftClient.io().send(raftMessage);

      if (reply == null || !reply.isSuccess()) {
        log.atSevere().log(
            "Failed request to raft with id %s and reply %s", raftClient.getId(), reply);
        throw new RuntimeException("Failed request to raft");
      }

      final ByteBuffer raftValue = reply.getMessage().getContent().asReadOnlyByteBuffer();
      return KVStoreRaftResponse.parseFrom(raftValue);
    } catch (InvalidProtocolBufferException e) {
      log.atSevere().log("Error parsing raft response", e);
      throw new RuntimeException("Error parsing raft response", e);
    } catch (IOException e) {
      log.atSevere().log("Error communicating to raft client", e);
      throw new RuntimeException("Error communicating to raft client", e);
    }
  }

  public static void put(long tid, String key, String value, RaftClient client) throws Exception {
    ProtoVKey protoVKey = ProtoVKey.newBuilder().setTid(tid).setKey(key).build();
    PutRequest putRequest =
        PutRequest.newBuilder().setKey(protoVKey).setValue(ByteString.copyFromUtf8(value)).build();
    KVStoreRaftRequest request = KVStoreRaftRequest.newBuilder().setPut(putRequest).build();
    RaftClientReply reply =
        client.io().send(Message.valueOf(convertToRatisByteString(request.toByteString())));

    if (reply == null || !reply.isSuccess()) {
      log.atSevere().log(
          "Failed to put request %s to server %s with reply %s", putRequest, client.getId(), reply);
      throw new RuntimeException("Faled to put request");
    }

    final ByteBuffer putValue = reply.getMessage().getContent().asReadOnlyByteBuffer();
    PutResponse response = PutResponse.parseFrom(putValue);
    log.atFine().log("Got put response: %s", response.toString());
  }

  public void put(int i, long tid, String key, String value) {
    try {
      put(tid, key, value, raftClient);
    } catch (Exception e) {
      throw new CompletionException(e);
    }
  }
}
