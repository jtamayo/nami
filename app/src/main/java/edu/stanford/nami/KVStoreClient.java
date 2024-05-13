package edu.stanford.nami;

import static edu.stanford.nami.ProtoUtils.convertToRatisByteString;

import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;

public final class KVStoreClient implements Closeable {
  private final KVStoreGrpc.KVStoreBlockingStub blockingStub;

  private final RaftClient client = newClient();

  @Override
  public void close() throws IOException {
    client.close();
  }

  // build the client
  static RaftClient newClient() {
    return RaftClient.newBuilder()
        .setProperties(new RaftProperties())
        .setRaftGroup(RaftConstants.RAFT_GROUP)
        .build();
  }

  public KVStoreClient(Channel channel) {
    blockingStub = KVStoreGrpc.newBlockingStub(channel);
  }

  public void get(long tid, String key) {
    ProtoVKey protoVKey = ProtoVKey.newBuilder().setTid(tid).setKey(key).build();
    GetRequest request = GetRequest.newBuilder().setKey(protoVKey).build();
    GetResponse response = blockingStub.get(request);
    System.out.println("Got get response: " + response.toString());
  }

  public static void put(long tid, String key, String value, RaftClient client) throws Exception {
    // use BlockingApi
    ProtoVKey protoVKey = ProtoVKey.newBuilder().setTid(tid).setKey(key).build();
    PutRequest putRequest =
        PutRequest.newBuilder().setKey(protoVKey).setValue(ByteString.copyFromUtf8(value)).build();
    KVStoreRequest request = KVStoreRequest.newBuilder().setPut(putRequest).build();
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
      put(tid, key, value, client);
    } catch (Exception e) {
      throw new CompletionException(e);
    }
  }

  /** Issues several different requests and then exits. */
  public static void main(String[] args) throws InterruptedException {
    String targetHost = "localhost";
    int defaultPort = 8980;
    if (args.length > 0) {
      final int serverPeerIndex = Integer.parseInt(args[0]);
      if (serverPeerIndex < 0 || serverPeerIndex > 2) {
        throw new IllegalArgumentException(
                "The server index must be 0, 1 or 2: peerIndex=" + serverPeerIndex);
      }
      defaultPort += serverPeerIndex;
    }
    String target = targetHost + ":" + defaultPort;

    ManagedChannel channel =
        Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
    try (KVStoreClient client = new KVStoreClient(channel)) {
      client.put(0, 1, "test1", "testvalue1");
      client.get(1, "test1");
      System.out.println("Finished getting the value");
    } catch (Throwable e) {
      e.printStackTrace();
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
