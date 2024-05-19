package edu.stanford.nami;

import static edu.stanford.nami.ProtoUtils.convertToRatisByteString;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.stanford.nami.config.PeersConfig;
import io.grpc.ManagedChannel;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;

public final class NamiClient implements Closeable {
  private final KVStoreGrpc.KVStoreBlockingStub blockingStub;
  private final RaftClient raftClient;

  @Override
  public void close() throws IOException {
    raftClient.close();
  }

  // build the client
  static RaftClient newRaftClient(PeersConfig peersConfig) {
    return RaftClient.newBuilder()
        .setProperties(new RaftProperties())
        .setRaftGroup(peersConfig.getRaftGroup())
        .build();
  }

  public NamiClient(ManagedChannel channel, PeersConfig peersConfig) {
    blockingStub = KVStoreGrpc.newBlockingStub(channel);
    raftClient = newRaftClient(peersConfig);
  }

  public long getRecentTid() {
    var request = GetRecentTidRequest.newBuilder().build();
    var response = blockingStub.getRecentTid(request);
    var recentTid = response.getTid();
    System.out.println("Recent TID: " + recentTid);
    return response.getTid();
  }

  public ByteString get(long tid, String key) {
    ProtoVKey protoVKey = ProtoVKey.newBuilder().setTid(tid).setKey(key).build();
    GetRequest request = GetRequest.newBuilder().setKey(protoVKey).build();
    GetResponse response = blockingStub.get(request);
    return response.getValue();
  }

  public TransactionResponse commit(
      long snapshotTid, Map<NKey, ByteString> readValues, Map<NKey, ByteString> writtenValues) {
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
  }

  private KVStoreRaftResponse submitRaftRequest(KVStoreRaftRequest request) {
    var raftMessage = Message.valueOf(convertToRatisByteString(request.toByteString()));

    try {
      RaftClientReply reply = raftClient.io().send(raftMessage);

      if (reply == null || !reply.isSuccess()) {
        var msg = "Failed request to raft with id " + raftClient.getId() + " with reply = " + reply;
        System.err.println(msg);
        throw new RuntimeException();
      }

      final ByteBuffer raftValue = reply.getMessage().getContent().asReadOnlyByteBuffer();
      return KVStoreRaftResponse.parseFrom(raftValue);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Error parsing raft response", e);
    } catch (IOException e) {
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
      System.err.println("Failed to get counter from " + client.getId() + " with reply = " + reply);
      return;
    }

    final ByteBuffer putValue = reply.getMessage().getContent().asReadOnlyByteBuffer();
    PutResponse response = PutResponse.parseFrom(putValue);
    System.out.println("Got put response: " + response.toString());
  }

  public void put(int i, long tid, String key, String value) {
    try {
      put(tid, key, value, raftClient);
    } catch (Exception e) {
      throw new CompletionException(e);
    }
  }
}
