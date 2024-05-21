package edu.stanford.nami;

import static edu.stanford.nami.ProtoUtils.convertToGoogleByteString;
import static edu.stanford.nami.ProtoUtils.convertToRatisByteString;

import com.google.protobuf.InvalidProtocolBufferException;
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

public class KVStoreStateMachine extends BaseStateMachine {
  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
  TransactionProcessor transactionProcessor;

  public KVStoreStateMachine(TransactionProcessor transactionProcessor) {
    this.transactionProcessor = transactionProcessor;
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
  public TransactionContext startTransaction(RaftClientRequest request)
      throws InvalidProtocolBufferException {
    try {
      System.out.println("Starting transaction");
      final ByteString content = request.getMessage().getContent();
      final com.google.protobuf.ByteString googleContent = convertToGoogleByteString(content);
      final KVStoreRaftRequest proto = KVStoreRaftRequest.parseFrom(googleContent);
      final TransactionContext.Builder b =
          TransactionContext.newBuilder().setStateMachine(this).setClientRequest(request);
      b.setLogData(content).setStateMachineContext(proto);
      System.out.println("Starting ending transaction");
      return b.build();
    } catch (Exception e) {
      System.out.println("Error starting transaction" + e.getMessage());
      e.printStackTrace();
      throw e;
    }
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

  private Message processTransaction(
      long currentTid, TransactionRequest request, boolean isLeader) {
    TransactionStatus status =
        this.transactionProcessor.processTransaction(request, currentTid, isLeader);
    ByteString byteString = constructTransactionResponse(status);
    return Message.valueOf(byteString);
  }

  private ByteString constructTransactionResponse(TransactionStatus status) {
    return convertToRatisByteString(
        KVStoreRaftResponse.newBuilder()
            .setTransaction(TransactionResponse.newBuilder().setStatus(status))
            .build()
            .toByteString());
  }

  private Message applyTransactionImpl(TransactionContext trx) {
    final RaftProtos.LogEntryProto entry = trx.getLogEntry();
    final long index = entry.getIndex();
    // TODO: Need to move this to after processing the transaction
    updateLastAppliedTermIndex(entry.getTerm(), index);

    final TermIndex termIndex = TermIndex.valueOf(entry);
    final KVStoreRaftRequest request = getProto(trx, entry);

    // if leader, log the transaction and the term-index
    boolean isLeader = trx.getServerRole() == RaftProtos.RaftPeerRole.LEADER;
    if (isLeader) {
      System.out.println(termIndex + ": Applying transaction " + request.getRequestCase());
    }

    switch (request.getRequestCase()) {
      case TRANSACTION:
        return processTransaction(index, request.getTransaction(), isLeader);
      default:
        System.err.println(getId() + ": Unexpected request case " + request.getRequestCase());
        throw new IllegalArgumentException(getId() + ": Unexpected request case " + request.getRequestCase());
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
    try {
      var message = applyTransactionImpl(trx);
      System.out.println("Done applying transaction");
      return CompletableFuture.completedFuture(message);
    } catch (Exception e) {
      e.printStackTrace();
      return JavaUtils.completeExceptionally(e);
    }
  }
}
