package edu.stanford.nami;

import com.google.protobuf.ByteString;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.TimeDuration;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class KVStoreServer {
  private final int port;
  private final Server server;
  private final RaftServer raftServer;

  public KVStoreServer(
      int port, RocksDB db, RaftPeer peer, File storageDir, TimeDuration simulatedSlowness)
      throws IOException {
    this(
        Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create()),
        port,
        db,
        peer,
        storageDir,
        simulatedSlowness);
  }

  public KVStoreServer(
      ServerBuilder<?> serverBuilder,
      int port,
      RocksDB db,
      RaftPeer peer,
      File storageDir,
      TimeDuration simulatedSlowness)
      throws IOException {
    this.port = port;
    VersionedKVStore kvStore = new VersionedKVStore(db);
    server = serverBuilder.addService(new KVStoreService(kvStore)).build();

    // create a property object
    final RaftProperties properties = new RaftProperties();

    // set the storage directory (different for each peer) in the RaftProperty object
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    // set the port (different for each peer) in RaftProperty object
    final int raftPeerPort = NetUtils.createSocketAddr(peer.getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(properties, raftPeerPort);

    // create the counter state machine which holds the counter value
    final KVStoreStateMachine stateMachine = new KVStoreStateMachine(simulatedSlowness, kvStore);

    // build the Raft server
    this.raftServer =
        RaftServer.newBuilder()
            .setGroup(RaftConstants.RAFT_GROUP)
            .setProperties(properties)
            .setServerId(peer.getId())
            .setStateMachine(stateMachine)
            .build();
  }

  /** Start serving requests. */
  public void start() throws IOException {
    server.start();
    System.out.println("Server started, listening on " + port);
    raftServer.start();
    System.out.println("Raft Server started, with id " + raftServer.getId());
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                  KVStoreServer.this.stop();
                } catch (InterruptedException | IOException e) {
                  e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
              }
            });
  }

  /** Stop serving requests and shutdown resources. */
  public void stop() throws InterruptedException, IOException {
    if (raftServer != null) {
      raftServer.close();
    }
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /** Await termination on the main thread since the grpc library uses daemon threads. */
  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException(
          "Invalid argument number: expected to be 1 but actual is " + args.length);
    }
    final int peerIndex = Integer.parseInt(args[0]);
    if (peerIndex < 0 || peerIndex > 2) {
      throw new IllegalArgumentException(
          "The server index must be 0, 1 or 2: peerIndex=" + peerIndex);
    }
    TimeDuration simulatedSlowness =
        Optional.ofNullable(RaftConstants.SIMULATED_SLOWNESS)
            .map(slownessList -> slownessList.get(peerIndex))
            .orElse(TimeDuration.ZERO);

    RocksDB.loadLibrary();
    try (final Options options = new Options()) {
      options.setCreateIfMissing(true);
      try (final RocksDB db = RocksDB.open(options, "/Users/lillianma/src/dbs/test" + peerIndex)) {
        // get peer and define storage dir
        final RaftPeer currentPeer = RaftConstants.PEERS.get(peerIndex);
        System.out.println("current Peer is " + currentPeer.getAddress());
        System.out.println("current Peer s client address is " + currentPeer.getClientAddress());
        final File storageDir = new File("./" + currentPeer.getId());
        KVStoreServer server =
            new KVStoreServer(8980 + peerIndex, db, currentPeer, storageDir, simulatedSlowness);
        server.start();
        server.blockUntilShutdown();
      }
    }
  }

  private static class KVStoreService extends KVStoreGrpc.KVStoreImplBase {
    private final VersionedKVStore kvStore;

    KVStoreService(VersionedKVStore kvStore) {
      this.kvStore = kvStore;
    }

    @Override
    public void get(
        GetRequest request, StreamObserver<edu.stanford.nami.GetResponse> responseObserver) {
      try {
        NVKey nvKey = new NVKey(request.getKey().getTid(), request.getKey().getKey());
        byte[] value = this.kvStore.get(nvKey);
        GetResponse response;
        if (value == null) {
          response = GetResponse.newBuilder().build();
        } else {
          response = GetResponse.newBuilder().setValue(ByteString.copyFrom(value)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } catch (RocksDBException e) {
        System.out.println("Error getting:" + e.getMessage());
        responseObserver.onError(e);
      }
    }
  }
}
