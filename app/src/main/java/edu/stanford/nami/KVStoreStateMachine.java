package edu.stanford.nami;

import static edu.stanford.nami.ProtoUtils.convertToGoogleByteString;
import static edu.stanford.nami.ProtoUtils.convertToRatisByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
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

public class KVStoreStateMachine extends BaseStateMachine {
  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

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
      return KVStoreRaftRequest.parseFrom(com.google.protobuf.ByteString.copyFrom(readOnlyByteBuffer));
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Failed to parse data, entry=" + entry, e);
    }
  }

  private CompletableFuture<Message> put(long index, PutRequest request) {
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
        return put(index, request.getPut());
      default:
        System.err.println(getId() + ": Unexpected request case " + request.getRequestCase());
        return JavaUtils.completeExceptionally(
            new IllegalArgumentException(
                getId() + ": Unexpected request case " + request.getRequestCase()));
    }
  }
}
