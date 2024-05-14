package edu.stanford.nami.client;

import edu.stanford.nami.NKey;
import java.nio.ByteBuffer;

public final class ClientTransaction {
  /** tid as of which all reads are done */
  private final long snapshotTid;

  private ClientTransaction(long snapshotTid) {
    this.snapshotTid = snapshotTid;
  }

  /*
     * get(Pkey): returns value at the latest committed version - return will include value and version?
  put(Pkey, value): Puts value into Pkey at next available version - return will include value and version?
  startTransaction(): returns a ClientTransaction object with the latest committed tid

  ClientTransaction.commit(): assembles a <> object, sends it through RAFT client. Returns status (COMMITTED/ABORTED/ERROR) and tid.

     */
  public ByteBuffer get(NKey key) {
    // get value as of snapshotTid and store the read key/value in memory so we can send it to
    // server later
    // TODO what do we do if the key doesn't exist? do we handle "null values"?
    throw new RuntimeException("not implemented");
  }

  public void put(NKey key, ByteBuffer value) {
    // store the value in memory under the given key so we can send it to the server later
  }

  public CommitOutcome commit() {
    throw new RuntimeException("not implemented");
  }

  public static ClientTransaction begin(NamiConnection connection) {
    // TODO connect to nami, get a recent snapshot tid, pass it here
    return new ClientTransaction(0);
  }

  enum CommitOutcome {
    /** All the writes in the transaction were applied. */
    COMMITTED,
    /**
     * None of the writes in the transaction were applied because the transaction conflicted with
     * others that came before it. It's ok to retry the entire application logic.
     */
    CONFLICTING_ABORTED,
    /**
     * The server connection died before this client discovered the outcome of this transaction. It
     * may or may not have been applied. It's not safe to retry the application operation, and an
     * error should be shown.
     */
    UNKNOWN,
  }
}
