package edu.stanford.nami.examples;

import edu.stanford.nami.NamiClient;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.util.concurrent.TimeUnit;

public class SimpleTestApp {

  /** Issues several different requests and then exits. */
  public static void main(String[] args) throws InterruptedException {
    // String targetHost = "localhost";
    // int defaultPort = 8980;
    // if (args.length > 0) {
    //   final int serverPeerIndex = Integer.parseInt(args[0]);
    //   if (serverPeerIndex < 0 || serverPeerIndex > 2) {
    //     throw new IllegalArgumentException(
    //         "The server index must be 0, 1 or 2: peerIndex=" + serverPeerIndex);
    //   }
    //   defaultPort += serverPeerIndex;
    // }
    // String target = targetHost + ":" + defaultPort;

    // ManagedChannel channel =
    //     Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
    // try (NamiClient client = new NamiClient(channel)) {
    //   client.put(0, 1, "test1", "testvalue1");
    //   client.get(1, "test1");
    //   System.out.println("Finished getting the value");
    // } catch (Throwable e) {
    //   e.printStackTrace();
    // } finally {
    //   channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    // }
  }
}
