package edu.stanford.nami;

import com.google.protobuf.ByteString;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class KVStoreServer {
  private final int port;
  private final Server server;

  public KVStoreServer(int port, RocksDB db) {
    this(Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create()), port, db);
  }

  public KVStoreServer(ServerBuilder<?> serverBuilder, int port, RocksDB db) {
    this.port = port;
    server = serverBuilder.addService(new KVStoreService(db)).build();
  }

  /** Start serving requests. */
  public void start() throws IOException {
    server.start();
    System.out.println("Server started, listening on " + port);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                  KVStoreServer.this.stop();
                } catch (InterruptedException e) {
                  e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
              }
            });
  }

  /** Stop serving requests and shutdown resources. */
  public void stop() throws InterruptedException {
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
    RocksDB.loadLibrary();
    try (final Options options = new Options()) {
      options.setCreateIfMissing(true);
      try (final RocksDB db = RocksDB.open(options, "/Users/lillianma/src/dbs/test1")) {
        KVStoreServer server = new KVStoreServer(8980, db);
        server.start();
        server.blockUntilShutdown();
      }
    }
  }

  private static class KVStoreService extends KVStoreGrpc.KVStoreImplBase {
    private final VersionedKVStore kvStore;

    KVStoreService(RocksDB db) {
      this.kvStore = new VersionedKVStore(db);
    }

    @Override
    public void get(
        GetRequest request, StreamObserver<edu.stanford.nami.GetResponse> responseObserver) {
      try {
        NVKey nvKey = new NVKey(request.getKey().getTid(), request.getKey().getKey());
        byte[] value = this.kvStore.get(nvKey);
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
    public void put(
        PutRequest request, StreamObserver<edu.stanford.nami.PutResponse> responseObserver) {
      try {
        NVKey nvKey = new NVKey(request.getKey().getTid(), request.getKey().getKey());
        ByteString value = request.getValue();
        this.kvStore.put(nvKey, value.toByteArray());
        PutResponse response = PutResponse.newBuilder().setValue(value).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } catch (RocksDBException e) {
        System.out.println("Error putting:" + e.getMessage());
        responseObserver.onError(e);
      }
    }
  }
}
