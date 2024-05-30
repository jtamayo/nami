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
import lombok.extern.flogger.Flogger;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

@Flogger
public class NamiServer {
  private final int port;
  private final Server server;
  private final RaftServer raftServer;
  private final KVStoreStateMachine stateMachine;

  public NamiServer(
      int port,
      RocksDB db,
      PeersConfig peersConfig,
      PeersConfig.PeerConfig peerConfig,
      ChunksConfig chunksConfig,
      Chunks.PeerAllocation peerAllocation,
      File storageDir)
      throws IOException {
    this.port = port;
    var serverBuilder = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create());
    VersionedKVStore kvStore = new VersionedKVStore(db, peerAllocation);
    RemoteStore remoteStore = new RemoteStore(peerConfig.getPeerId(), peersConfig, chunksConfig);
    CachingStore cachingStore = new CachingStore();
    TransactionProcessor transactionProcessor =
        new TransactionProcessor(kvStore, remoteStore, cachingStore);
    stateMachine = new KVStoreStateMachine(transactionProcessor);
    server =
        serverBuilder.addService(new KVStoreService(kvStore, remoteStore, stateMachine)).build();

    // create a property object
    final RaftProperties properties = new RaftProperties();

    // set the storage directory (different for each peer) in the RaftProperty object
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    // Enable triggering snapshot automatically when log size exceeds limit
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, 2500);

    // customize snapshot creation gap
    RaftServerConfigKeys.Snapshot.setCreationGap(properties, 1024L);

    // Disable installing snapshot in a new follower
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, true);

    // set the port (different for each peer) in RaftProperty object
    final int raftPeerPort = peerConfig.getRaftPort();
    GrpcConfigKeys.Server.setPort(properties, raftPeerPort);

    // build the Raft server
    this.raftServer =
        RaftServer.newBuilder()
            .setGroup(peersConfig.getRaftGroup())
            .setProperties(properties)
            .setOption(RaftStorage.StartupOption.RECOVER)
            .setServerId(RaftPeerId.valueOf(peerConfig.getPeerId()))
            .setStateMachine(stateMachine)
            .build();
  }

  /** Start serving requests. */
  public void start() throws IOException {
    log.atInfo().log("Starting NamiServer gRPC listening on port %s", port);
    server.start();
    log.atInfo().log(
        "Starting NamiServer raft with id %s and config %s",
        raftServer.getId(), raftServer.getDivision(PeersConfig.getRaftGroupId()));
    raftServer.start();
    log.atInfo().log("NamiServer started!");

    // make sure we shut down properly
    Runtime.getRuntime().addShutdownHook(new ShutdownHook());
  }

  /** Stop serving requests and shutdown resources. */
  public void stop() throws InterruptedException, IOException {
    if (raftServer != null) {
      log.atWarning().log("Shutting down Raft Server");
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
    System.out.println("NamiServer Running in " + (new File(".").getAbsolutePath()));
    System.setProperty(
        "flogger.backend_factory",
        "com.google.common.flogger.backend.slf4j.Slf4jBackendFactory#getInstance");

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
        peersConfig.getPeers().stream()
            .filter(pc -> pc.getPeerId().equals(selfPeerId))
            .findAny()
            .orElse(null);
    if (peerConfig == null) {
      throw new RuntimeException("Could not find peer config for " + selfPeerId);
    }
    var chunksConfig = loadChunksConfig(configFile, config.getChunkConfigPath());
    var peerAllocation =
        chunksConfig.getPeerAllocations().stream()
            .filter(pa -> pa.peerId().equals(selfPeerId))
            .findAny()
            .orElse(null);
    if (peerAllocation == null) {
      throw new RuntimeException("Could not find peer allocation for " + selfPeerId);
    }
    var dataPath =
        configFile
            .getParentFile()
            .toPath()
            .resolve(config.getDataPath())
            .normalize()
            .resolve(selfPeerId);
    log.atInfo().log("All data will be stored in %s", dataPath.toAbsolutePath());
    if (!dataPath.toFile().exists()) {
      log.atInfo().log("Creating data folder %s", dataPath.toAbsolutePath());
      dataPath.toFile().mkdirs();
    }
    var rocksDbPath = dataPath.resolve("rocksdb");
    var raftPath = dataPath.resolve("raft");
    log.atInfo().log("RocksDB data will be stored in %s", rocksDbPath.toAbsolutePath());
    log.atInfo().log("Raft data will be stored in %s", raftPath.toAbsolutePath());

    RocksDB.loadLibrary();
    try (final Options options = new Options()) {
      options.setCreateIfMissing(true);
      try (var db = RocksDB.open(options, rocksDbPath.toString())) {
        // get peer and define storage dir
        log.atInfo().log("Current peer is %s", peerConfig.getRaftAddress());
        var storageDir = raftPath.toFile();
        var server =
            new NamiServer(
                peerConfig.getKvPort(),
                db,
                peersConfig,
                peerConfig,
                chunksConfig,
                peerAllocation,
                storageDir);
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
    private final RemoteStore remoteStore;
    private final KVStoreStateMachine stateMachine;

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
      log.atFine().log("gRPC GetRequest %s", request);
      NKey nKey = new NKey(request.getKey().getKey());
      byte[] value;
      try {
        if (this.kvStore.hasKeyInAllocation(nKey)) {
          var tid = request.getKey().getTid();
          this.kvStore.waitUtilTid(tid, 5000);
          value = this.kvStore.getAsOf(nKey, tid);
        } else {
          log.atWarning().log(
              "Client asked for a key that is not in this store's allocation, key %s", nKey);
          throw new RuntimeException(
              "Client asked for a key that is not in this store's allocation");
        }
        Preconditions.checkNotNull(value);
        GetResponse response =
            GetResponse.newBuilder().setValue(ByteString.copyFrom(value)).build();
        log.atFine().log("Responding to key %s with value %s", nKey, response);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } catch (RocksDBException e) {
        log.atSevere().log("Error processing request %s", request, e);
        responseObserver.onError(e);
      } catch (InterruptedException e) {
        log.atWarning().log("Interrupted while processing request %s", request, e);
        responseObserver.onError(e);
      }
    }

    @Override
    public void getRecentTid(
        GetRecentTidRequest request, StreamObserver<GetRecentTidResponse> responseObserver) {
      var latestTid = this.kvStore.getLatestTid();
      var response = GetRecentTidResponse.newBuilder().setTid(latestTid).build();
      log.atFine().log("gRPC GetRecentTid request, responding with %s", response);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  private class ShutdownHook extends Thread {
    @Override
    public void run() {
      // Use stderr here since the logger may have been reset by its JVM shutdown hook.
      System.err.println("*** shutting down gRPC server since JVM is shutting down ***");
      try {
        NamiServer.this.stop();
      } catch (InterruptedException | IOException e) {
        e.printStackTrace(System.err);
      }
      System.err.println("*** server shut down ***");
    }
  }
}
