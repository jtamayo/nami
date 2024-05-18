package edu.stanford.nami;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import edu.stanford.nami.config.ChunksConfig;
import edu.stanford.nami.config.PeersConfig;
import edu.stanford.nami.config.ServerConfig;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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

public class NamiServer {
  private final int port;
  private final Server server;
  private final RaftServer raftServer;

  public NamiServer(
      int port, RocksDB db, RaftPeer peer, File storageDir, TimeDuration simulatedSlowness)
      throws IOException {
    this.port = port;
    var serverBuilder = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create());
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

    // make sure we shut down properly
    Runtime.getRuntime().addShutdownHook(new ShutdownHook());
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
    System.out.println("Running in " + (new File(".").getAbsolutePath()));

    if (args.length != 1) {
      System.err.println("Invalid usage. Usage: k-v-store-server <config_file>");
      System.exit(-1);
    }
    var configFileName = args[0];
    var configFile = new File(configFileName);

    if (!configFile.exists()) {
      System.err.println("File " + configFile.getAbsolutePath() + " does not exist");
      System.exit(-2);
    } else {
      System.out.println("Found config file at " + configFile.getAbsolutePath());
    }

    var config = loadServerConfig(configFile);
    System.out.println("Loaded server config " + config);
    var selfPeerId = config.getSelfPeerId();
    var peersConfig = loadPeersConfig(configFile, config.getPeerConfigsPath());
    var chunksConfig = loadChunksConfig(configFile, config.getChunkConfigPath());
    var dataPath =
        configFile
            .getParentFile()
            .toPath()
            .resolve(config.getDataPath())
            .normalize()
            .resolve(selfPeerId);
    System.out.println("All data will be stored in " + dataPath.toAbsolutePath());
    if (!dataPath.toFile().exists()) {
      System.out.println("Creating data folder " + dataPath.toAbsolutePath());
      dataPath.toFile().mkdirs();
    }
    var rocksDbPath = dataPath.resolve("rocksdb");
    var raftPath = dataPath.resolve("raft");
    System.out.println("RocksDB data will be stored in " + rocksDbPath.toAbsolutePath());
    System.out.println("Raft data will be stored in " + raftPath.toAbsolutePath());

    final int peerIndex = config.getPeerIndex();
    var simulatedSlowness =
        Optional.ofNullable(RaftConstants.SIMULATED_SLOWNESS)
            .map(slownessList -> slownessList.get(peerIndex))
            .orElse(TimeDuration.ZERO);

    RocksDB.loadLibrary();
    try (final Options options = new Options()) {
      options.setCreateIfMissing(true);
      try (var db = RocksDB.open(options, rocksDbPath.toString())) {
        // get peer and define storage dir
        final RaftPeer currentPeer = RaftConstants.PEERS.get(peerIndex);
        System.out.println("current Peer is " + currentPeer.getAddress());
        System.out.println("current Peer s client address is " + currentPeer.getClientAddress());
        var storageDir = raftPath.toFile();
        var server =
            new NamiServer(8980 + peerIndex, db, currentPeer, storageDir, simulatedSlowness);
        server.start();
        server.blockUntilShutdown();
      }
    }
  }

  private static ServerConfig loadServerConfig(File configFile) {
    try (var reader = Files.newReader(configFile, Charsets.UTF_8)) {
      return new Gson().fromJson(reader, ServerConfig.class);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static PeersConfig loadPeersConfig(File configFile, String path) {
    return loadConfig(configFile, path, PeersConfig.class);
  }

  public static ChunksConfig loadChunksConfig(File configFile, String path) {
    return loadConfig(configFile, path, ChunksConfig.class);
  }

  public static <T> T loadConfig(File configFile, String path, Class<T> clazz) {
    var file = configFile.getParentFile().toPath().resolve(path).toFile();
    try (var reader = Files.newReader(file, Charsets.UTF_8)) {
      return new Gson().fromJson(reader, clazz);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class KVStoreService extends KVStoreGrpc.KVStoreImplBase {
    private final VersionedKVStore kvStore;

    KVStoreService(VersionedKVStore kvStore) {
      this.kvStore = kvStore;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
      // TODO interact with kv store
      var buffer = ByteBuffer.allocate(8);
      buffer.putLong(0);
      buffer.rewind();
      var response = GetResponse.newBuilder().setValue(ByteString.copyFrom(buffer)).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      // try {
      //   NVKey nvKey = new NVKey(request.getKey().getTid(), request.getKey().getKey());
      //   byte[] value = this.kvStore.get(nvKey);
      //   GetResponse response;
      //   if (value == null) {
      //     response = GetResponse.newBuilder().build();
      //   } else {
      //     response = GetResponse.newBuilder().setValue(ByteString.copyFrom(value)).build();
      //   }
      //   responseObserver.onNext(response);
      //   responseObserver.onCompleted();
      // } catch (RocksDBException e) {
      //   System.out.println("Error getting:" + e.getMessage());
      //   responseObserver.onError(e);
      // }
    }

    @Override
    public void getRecentTid(
        GetRecentTidRequest request, StreamObserver<GetRecentTidResponse> responseObserver) {
      // TODO expose a recent tid
      var response = GetRecentTidResponse.newBuilder().setTid(1).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  private class ShutdownHook extends Thread {
    @Override
    public void run() {
      // Use stderr here since the logger may have been reset by its JVM shutdown hook.
      System.err.println("*** shutting down gRPC server since JVM is shutting down");
      try {
        NamiServer.this.stop();
      } catch (InterruptedException | IOException e) {
        e.printStackTrace(System.err);
      }
      System.err.println("*** server shut down");
    }
  }
}
