package edu.stanford.nami.nativerocksdb;

import edu.stanford.nami.*;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.flogger.Flogger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

@Flogger
public class NativeRocksDBServer {
  @Getter private final int port;
  private final Server server;

  public NativeRocksDBServer(int port, RocksDB db) {
    this.port = port;
    var serverBuilder = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create());
    server = serverBuilder.addService(new NativeRocksDBServer.NativeRocksDBService()).build();
  }

  /** Start serving requests. */
  public void start() throws IOException {
    log.atInfo().log("Starting NativeRocksDB gRPC listening on port %s", port);
    server.start();
    log.atInfo().log("NativeRocksDB started!");

    // make sure we shut down properly
    Runtime.getRuntime().addShutdownHook(new NativeRocksDBServer.ShutdownHook());
  }

  /** Stop serving requests and shutdown resources. */
  public void stop() throws InterruptedException, IOException {
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
    System.out.println("NativeRocksDBServer Running in " + (new File(".").getAbsolutePath()));
    System.setProperty(
        "flogger.backend_factory",
        "com.google.common.flogger.backend.slf4j.Slf4jBackendFactory#getInstance");

    if (args.length != 2) {
      System.err.println("Invalid usage. Usage: k-v-store-server <port> <data_dir_path>");
      System.exit(-1);
    }
    var port = Integer.parseInt(args[0]);
    var dataPath = args[1];
    var dataDir = new File(dataPath);

    log.atInfo().log("All data will be stored in %s", dataPath);
    if (!dataDir.exists()) {
      log.atInfo().log("Creating data folder %s", dataPath);
      dataDir.mkdirs();
    }
    var rocksDbPath = dataDir.toPath().resolve("rocksdb");
    log.atInfo().log("RocksDB data will be stored in %s", rocksDbPath.toAbsolutePath());
    RocksDB.loadLibrary();
    try (final Options options = new Options()) {
      options.setCreateIfMissing(true);
      try (var db = RocksDB.open(options, rocksDbPath.toString())) {
        var server = new NativeRocksDBServer(port, db);
        server.start();
        server.blockUntilShutdown();
      }
    }
  }

  @RequiredArgsConstructor
  private static class NativeRocksDBService extends NativeRocksDBGrpc.NativeRocksDBImplBase {
    public void beginTransaction(
        NativeRocksBeginTransactionRequest request,
        StreamObserver<NativeRocksBeginTransactionResponse> responseObserver) {
      log.atFine().log("gRPC NativeRocksBeginTransactionRequest %s", request);
      var responseBuilder = NativeRocksBeginTransactionResponse.newBuilder();
      // TODO: change this
      responseBuilder.setTid(1L);
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }

    public void getForUpdateInTransaction(
        NativeRocksInTransactionGetForUpdateRequest request,
        StreamObserver<NativeRocksInTransactionGetForUpdateResponse> responseObserver) {
      log.atFine().log("gRPC NativeRocksInTransactionGetForUpdateRequest %s", request);
      var responseBuilder = NativeRocksInTransactionGetForUpdateResponse.newBuilder();
      // TODO: change this
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }

    public void putIntransaction(
        NativeRocksInTransactionPutRequest request,
        StreamObserver<NativeRocksInTransactionPutResponse> responseObserver) {
      log.atFine().log("gRPC NativeRocksInTransactionPutRequest %s", request);
      var responseBuilder = NativeRocksInTransactionPutResponse.newBuilder();
      // TODO: change this
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }

    public void commitTransaction(
        NativeRocksTransactionCommitRequest request,
        StreamObserver<NativeRocksTransactionCommitResponse> responseObserver) {
      log.atFine().log("gRPC NativeRocksTransactionCommitRequest %s", request);
      var responseBuilder = NativeRocksTransactionCommitResponse.newBuilder();
      // TODO: change this
      responseBuilder.setStatus(NativeRocksTransactionStatus.COMMITTED);
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }
  }

  private class ShutdownHook extends Thread {
    @Override
    public void run() {
      // Use stderr here since the logger may have been reset by its JVM shutdown hook.
      System.err.println("*** shutting down gRPC server since JVM is shutting down ***");
      try {
        NativeRocksDBServer.this.stop();
      } catch (InterruptedException | IOException e) {
        e.printStackTrace(System.err);
      }
      System.err.println("*** server shut down ***");
    }
  }
}
