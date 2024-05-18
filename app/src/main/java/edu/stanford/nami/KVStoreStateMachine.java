package edu.stanford.nami;

import static edu.stanford.nami.ProtoUtils.convertToGoogleByteString;
import static edu.stanford.nami.ProtoUtils.convertToRatisByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

public class KVStoreStateMachine extends BaseStateMachine {
  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
  private static final int NUM_DB_RETRIES = 3;

  // For testing only
  private final TimeDuration simulatedSlowness;

  private final VersionedKVStore kvStore;

  public KVStoreStateMachine(TimeDuration simulatedSlowness, VersionedKVStore kvStore) {
    this.kvStore = kvStore;
    this.simulatedSlowness = simulatedSlowness.isPositive() ? simulatedSlowness : null;
  }

  private void addSleep() {
    if (simulatedSlowness != null) {
      try {
        simulatedSlowness.sleep();
      } catch (InterruptedException e) {
        System.out.println(
            "{}: get interrupted in simulated slowness sleep before apply transaction");
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Initialize the state machine storage and then load the state.
   *
   * @param server the server running this state machine
   * @param groupId the id of the {@link org.apache.ratis.protocol.RaftGroup}
   * @param raftStorage the storage of the server
   * @throws IOException if it fails to load the state.
   */
  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage)
      throws IOException {
    super.initialize(server, groupId, raftStorage);
    storage.init(raftStorage);
    reinitialize();
  }

  @Override
  public TransactionContext startTransaction(
      RaftProtos.LogEntryProto entry, RaftProtos.RaftPeerRole role) {
    return TransactionContext.newBuilder()
        .setStateMachine(this)
        .setLogEntry(entry)
        .setServerRole(role)
        .setStateMachineContext(getProto(entry))
        .build();
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
    System.out.println("Starting transaction");
    final ByteString content = request.getMessage().getContent();
    final com.google.protobuf.ByteString googleContent = convertToGoogleByteString(content);
    final KVStoreRaftRequest proto = KVStoreRaftRequest.parseFrom(googleContent);
    final TransactionContext.Builder b =
        TransactionContext.newBuilder().setStateMachine(this).setClientRequest(request);
    b.setLogData(content).setStateMachineContext(proto);
    return b.build();
  }

  static KVStoreRaftRequest getProto(TransactionContext context, RaftProtos.LogEntryProto entry) {
    if (context != null) {
      final KVStoreRaftRequest proto = (KVStoreRaftRequest) context.getStateMachineContext();
      if (proto != null) {
        return proto;
      }
    }
    return getProto(entry);
  }

  static KVStoreRaftRequest getProto(RaftProtos.LogEntryProto entry) {
    ByteString logData = entry.getStateMachineLogEntry().getLogData();
    ByteBuffer readOnlyByteBuffer = logData.asReadOnlyByteBuffer();
    try {
      return KVStoreRaftRequest.parseFrom(
          com.google.protobuf.ByteString.copyFrom(readOnlyByteBuffer));
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Failed to parse data, entry=" + entry, e);
    }
  }

  private CompletableFuture<Message> processPut(long index, PutRequest request) {
    NVKey nvKey = new NVKey(request.getKey().getTid(), request.getKey().getKey());
    com.google.protobuf.ByteString value = request.getValue();
    try {
      this.kvStore.put(nvKey, value.toByteArray());
      ByteString byteString =
          convertToRatisByteString(PutResponse.newBuilder().setValue(value).build().toByteString());
      return CompletableFuture.completedFuture(Message.valueOf(byteString));
    } catch (RocksDBException e) {
      System.out.println("Error putting:" + e.getMessage());
      return CompletableFuture.completedFuture(Message.EMPTY);
    }
  }

  private boolean processInTransactionGet(long tid, InTransactionGet inTransactionGet)
      throws RocksDBException {
    NKey nKey = new NKey(inTransactionGet.getKey());
    byte[] value;
    int numRetries = 0;
    while (true) {
      try {
        value = this.kvStore.getAsOf(nKey, tid);
        break;
      } catch (RocksDBException e) {
        // Fix this to capture transient errors vs un-retriable errors
        if (numRetries <= NUM_DB_RETRIES
            && (e.getStatus().getCode() == Status.Code.Aborted
                || e.getStatus().getCode() == Status.Code.Expired
                || e.getStatus().getCode() == Status.Code.TimedOut)) {
          numRetries++;
          System.out.println(
              "Retrying get: " + numRetries + " out of " + NUM_DB_RETRIES + " times");
          continue;
        }
        throw e;
      }
    }
    return value != null && Arrays.equals(value, inTransactionGet.getValue().toByteArray());
  }

  private void processInTransactionPut(long index, InTransactionPut inTransactionPut)
      throws RocksDBException {
    NVKey nvKey = new NVKey(index, inTransactionPut.getKey());
    com.google.protobuf.ByteString value = inTransactionPut.getValue();
    int numRetries = 0;
    while (true) {
      try {
        this.kvStore.put(nvKey, value.toByteArray());
        break;
      } catch (RocksDBException e) {
        // Fix this to capture transient errors vs un-retriable errors?
        if (numRetries <= NUM_DB_RETRIES
            && (e.getStatus().getCode() == Status.Code.Aborted
                || e.getStatus().getCode() == Status.Code.Expired
                || e.getStatus().getCode() == Status.Code.TimedOut)) {
          numRetries++;
          System.out.println("Retrying put: " + numRetries + " time");
          continue;
        }
        // Eventually give up?
        throw e;
      }
    }
  }

  private CompletableFuture<Message> processTransaction(long index, TransactionRequest request) {
    long snapshotTid = request.getSnapshotTid();
    for (InTransactionGet inTransactionGet : request.getReadsList()) {
      try {
        if (!this.processInTransactionGet(snapshotTid, inTransactionGet)) {
          ByteString byteString =
              convertToRatisByteString(
                  TransactionResponse.newBuilder()
                      .setStatus(TransactionStatus.CONFLICT_ABORTED)
                      .build()
                      .toByteString());
          return CompletableFuture.completedFuture(Message.valueOf(byteString));
        }
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
    for (InTransactionPut inTransactionPut : request.getPutsList()) {
      try {
        this.processInTransactionPut(index, inTransactionPut);
      } catch (RocksDBException e) {
        // A problem we need to handle if DB is down continuously?
        throw new RuntimeException(e);
      }
    }
    ByteString byteString =
        convertToRatisByteString(
            TransactionResponse.newBuilder()
                .setStatus(TransactionStatus.COMMITTED)
                .build()
                .toByteString());
    return CompletableFuture.completedFuture(Message.valueOf(byteString));
  }

  /**
   * Apply the Put request by adding the value to rocksdb
   *
   * @param trx the transaction context
   * @return the message containing the updated counter value
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    System.out.println("Applying transaction");
    final RaftProtos.LogEntryProto entry = trx.getLogEntry();
    final long index = entry.getIndex();
    updateLastAppliedTermIndex(entry.getTerm(), index);

    final TermIndex termIndex = TermIndex.valueOf(entry);
    final KVStoreRaftRequest request = getProto(trx, entry);

    // if leader, log the transaction and the term-index
    if (trx.getServerRole() == RaftProtos.RaftPeerRole.LEADER) {
      System.out.println(termIndex + ": Applying transaction " + request.getRequestCase());
    }

    // Testing only
    addSleep();

    switch (request.getRequestCase()) {
      case PUT:
        return processPut(index, request.getPut());
      case TRANSACTION:
        return processTransaction(index, request.getTransaction());
      default:
        System.err.println(getId() + ": Unexpected request case " + request.getRequestCase());
        return JavaUtils.completeExceptionally(
            new IllegalArgumentException(
                getId() + ": Unexpected request case " + request.getRequestCase()));
    }
  }
}
