package edu.stanford.nami;

import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.util.concurrent.TimeUnit;

public class KVStoreClient {
  private final KVStoreGrpc.KVStoreBlockingStub blockingStub;

  public KVStoreClient(Channel channel) {
    blockingStub = KVStoreGrpc.newBlockingStub(channel);
  }

  public void get(long tid, String key) {
    ProtoVKey protoVKey = ProtoVKey.newBuilder().setTid(tid).setKey(key).build();
    GetRequest request = GetRequest.newBuilder().setKey(protoVKey).build();
    GetResponse response = blockingStub.get(request);
    System.out.println("Got get response: " + response.toString());
  }

  public void put(long tid, String key, String value) {
    ProtoVKey protoVKey = ProtoVKey.newBuilder().setTid(tid).setKey(key).build();
    PutRequest request =
        PutRequest.newBuilder().setKey(protoVKey).setValue(ByteString.copyFromUtf8(value)).build();
    PutResponse response = blockingStub.put(request);
    System.out.println("Got put response: " + response.toString());
  }

  /** Issues several different requests and then exits. */
  public static void main(String[] args) throws InterruptedException {
    String target = "localhost:8980";
    if (args.length > 0) {
      if ("--help".equals(args[0])) {
        System.err.println("Usage: [target]");
        System.err.println("");
        System.err.println("  target  The server to connect to. Defaults to " + target);
        System.exit(1);
      }
      target = args[0];
    }

    ManagedChannel channel =
        Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
    try {
      KVStoreClient client = new KVStoreClient(channel);

      client.put(1, "test", "testvalue");

      System.out.println("Finished putting the value");

      client.get(1, "test");
      System.out.println("Finished getting the value");

    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
