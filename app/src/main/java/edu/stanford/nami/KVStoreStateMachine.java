package edu.stanford.nami;

import static edu.stanford.nami.ProtoUtils.convertToGoogleByteString;
import static edu.stanford.nami.ProtoUtils.convertToRatisByteString;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.flogger.Flogger;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MD5FileUtil;
import org.rocksdb.RocksDBException;

@Flogger
public class KVStoreStateMachine extends BaseStateMachine {
  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
  private final TransactionProcessor transactionProcessor;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  public KVStoreStateMachine(TransactionProcessor transactionProcessor) {
    this.transactionProcessor = transactionProcessor;
  }

  private AutoCloseableLock readLock() {
    return AutoCloseableLock.acquire(lock.readLock());
  }

  private AutoCloseableLock writeLock() {
    return AutoCloseableLock.acquire(lock.writeLock());
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
    loadSnapshot(storage.getLatestSnapshot());
  }

  @Override
  public void pause() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void reinitialize() throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
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
      log.atFine().log("Starting transaction");
      final ByteString content = request.getMessage().getContent();
      final com.google.protobuf.ByteString googleContent = convertToGoogleByteString(content);
      final KVStoreRaftRequest proto = KVStoreRaftRequest.parseFrom(googleContent);
      final TransactionContext.Builder b =
          TransactionContext.newBuilder().setStateMachine(this).setClientRequest(request);
      b.setLogData(content).setStateMachineContext(proto);
      log.atFine().log("Starting ending transaction");
      return b.build();
    } catch (Exception e) {
      log.atSevere().log("Error starting transaction" + e.getMessage());
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
    ByteString byteString = constructTransactionResponse(status, currentTid);
    return Message.valueOf(byteString);
  }

  private ByteString constructTransactionResponse(TransactionStatus status, long tid) {
    return convertToRatisByteString(
        KVStoreRaftResponse.newBuilder()
            .setTransaction(TransactionResponse.newBuilder().setStatus(status).setTid(tid))
            .build()
            .toByteString());
  }

  private Message applyTransactionImpl(TransactionContext trx) {
    RaftProtos.LogEntryProto entry = trx.getLogEntry();
    long logEntryIndex = entry.getIndex();
    KVStoreRaftRequest request = getProto(trx, entry);

    // if leader, log the transaction and the term-index
    boolean isLeader = trx.getServerRole() == RaftProtos.RaftPeerRole.LEADER;
    if (isLeader) {
      log.atFine().log(
          TermIndex.valueOf(entry) + ": Applying transaction " + request.getRequestCase());
    }

    // HACK we want tids to start at 1, so we're always certain they've been
    // included properly in proto messages. Unfortunately, log entry indices
    // start at 0.
    // So work around it by adding one to derive the tid :/
    long currentTid = logEntryIndex + 1;
    Message message;
    switch (request.getRequestCase()) {
      case TRANSACTION:
        message = processTransaction(currentTid, request.getTransaction(), isLeader);
        break;
      default:
        log.atSevere().log(getId() + ": Unexpected request case " + request.getRequestCase());
        throw new IllegalArgumentException(
            getId() + ": Unexpected request case " + request.getRequestCase());
    }

    updateLastAppliedTermIndex(entry.getTerm(), logEntryIndex);
    // Update the latest tid after the last applied term is applied
    this.transactionProcessor.getKvStore().updateLatestTid(currentTid);

    return message;
  }

  /**
   * Apply the Put request by adding the value to rocksdb
   *
   * @param trx the transaction context
   * @return the message containing the updated counter value
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    log.atFine().log("Applying transaction");
    try {
      var message = applyTransactionImpl(trx);
      log.atFine().log("Done applying transaction");
      return CompletableFuture.completedFuture(message);
    } catch (Exception e) {
      e.printStackTrace();
      return JavaUtils.completeExceptionally(e);
    }
  }

  @Override
  public long takeSnapshot() {
    final TermIndex snapshotTermIndex;
    final long snapshotTid;
    try (AutoCloseableLock readLock = readLock()) {
      snapshotTid = this.transactionProcessor.getKvStore().getLatestTid();
      snapshotTermIndex = getLastAppliedTermIndex();
      this.transactionProcessor.getKvStore().flush();
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }

    final File snapshotFile =
        storage.getSnapshotFile(snapshotTermIndex.getTerm(), snapshotTermIndex.getIndex());
    log.atInfo().log("Taking a snapshot to file " + snapshotFile);

    try (ObjectOutputStream out =
        new ObjectOutputStream(new BufferedOutputStream(FileUtils.newOutputStream(snapshotFile)))) {
      out.writeObject(snapshotTid);
    } catch (IOException ioe) {
      log.atWarning().log(
          "Failed to write snapshot file \""
              + snapshotFile
              + "\", last applied index="
              + snapshotTermIndex);
    }

    final MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
    final FileInfo info = new FileInfo(snapshotFile.toPath(), md5);
    storage.updateLatestSnapshot(new SingleFileSnapshotInfo(info, snapshotTermIndex));
    return snapshotTermIndex.getIndex();
  }

  public long loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
    if (snapshot == null) {
      log.atWarning().log("The snapshot info is null.");
      return RaftLog.INVALID_LOG_INDEX;
    }
    final File snapshotFile = snapshot.getFile().getPath().toFile();
    if (!snapshotFile.exists()) {
      log.atWarning().log(
          "The snapshot file " + snapshot + " does not exist for snapshot " + snapshot);
      return RaftLog.INVALID_LOG_INDEX;
    }

    // verify md5
    final MD5Hash md5 = snapshot.getFile().getFileDigest();
    if (md5 != null) {
      MD5FileUtil.verifySavedMD5(snapshotFile, md5);
    }
    log.atInfo().log("Verified MD5 for snapshot " + snapshot);

    final TermIndex snapshotTermIndex =
        SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
    try (AutoCloseableLock writeLock = writeLock();
        ObjectInputStream in =
            new ObjectInputStream(
                new BufferedInputStream(FileUtils.newInputStream(snapshotFile)))) {
      setLastAppliedTermIndex(snapshotTermIndex);
      long snapshotTid = JavaUtils.cast(in.readObject());
      this.transactionProcessor.getKvStore().resetLatestTid(snapshotTid);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Failed to load " + snapshot, e);
    }
    return snapshotTermIndex.getIndex();
  }
}
