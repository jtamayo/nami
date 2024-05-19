package edu.stanford.nami;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import edu.stanford.nami.config.ChunksConfig;
import edu.stanford.nami.config.Config;
import edu.stanford.nami.config.PeersConfig;
import edu.stanford.nami.config.ServerConfig;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class NamiServer {
  private final int port;
  private final Server server;
  private final RaftServer raftServer;

  public NamiServer(int port, RocksDB db, PeersConfig peersConfig, PeersConfig.PeerConfig peerConfig, File storageDir)
      throws IOException {
    this.port = port;
    var serverBuilder = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create());
    VersionedKVStore kvStore = new VersionedKVStore(db);
    KVStoreStateMachine stateMachine = new KVStoreStateMachine(kvStore);
    server = serverBuilder.addService(new KVStoreService(kvStore, stateMachine)).build();

    // create a property object
    final RaftProperties properties = new RaftProperties();

    // set the storage directory (different for each peer) in the RaftProperty object
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    // set the port (different for each peer) in RaftProperty object
    final int raftPeerPort = peerConfig.getRaftPort();
    GrpcConfigKeys.Server.setPort(properties, raftPeerPort);

    // build the Raft server
    this.raftServer =
        RaftServer.newBuilder()
            .setGroup(peersConfig.getRaftGroup())
            .setProperties(properties)
            .setServerId(RaftPeerId.valueOf(peerConfig.getPeerId()))
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
    PeersConfig.PeerConfig peerConfig =
            peersConfig.getPeers().stream().filter(pc -> pc.getPeerId().equals(selfPeerId)).findAny().orElse(null);
    if (peerConfig == null) {
      throw new RuntimeException("Could not find peer config for " + selfPeerId);
    }
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

    RocksDB.loadLibrary();
    try (final Options options = new Options()) {
      options.setCreateIfMissing(true);
      try (var db = RocksDB.open(options, rocksDbPath.toString())) {
        // get peer and define storage dir
        System.out.println("current Peer is " + peerConfig.getAddress() + ":" + peerConfig.getKvPort());
        var storageDir = raftPath.toFile();
        var server = new NamiServer(peerConfig.getKvPort(), db, peersConfig, peerConfig, storageDir);
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
    return Config.loadConfig(configFile, path, PeersConfig.class);
  }

  public static ChunksConfig loadChunksConfig(File configFile, String path) {
    return Config.loadConfig(configFile, path, ChunksConfig.class);
  }

  @RequiredArgsConstructor
  private static class KVStoreService extends KVStoreGrpc.KVStoreImplBase {
    private final VersionedKVStore kvStore;
    private final KVStoreStateMachine stateMachine;

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
      try {
        byte[] value =
            this.kvStore.getAsOf(new NKey(request.getKey().getKey()), request.getKey().getTid());
        Preconditions.checkNotNull(value);
        GetResponse response =
            GetResponse.newBuilder().setValue(ByteString.copyFrom(value)).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } catch (RocksDBException e) {
        System.out.println("Error getting:" + e.getMessage());
        responseObserver.onError(e);
      }
    }

    @Override
    public void getRecentTid(
        GetRecentTidRequest request, StreamObserver<GetRecentTidResponse> responseObserver) {
      TermIndex lastAppliedTermIndex = this.stateMachine.getLastAppliedTermIndex();
      var response =
          GetRecentTidResponse.newBuilder().setTid(lastAppliedTermIndex.getIndex()).build();
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
