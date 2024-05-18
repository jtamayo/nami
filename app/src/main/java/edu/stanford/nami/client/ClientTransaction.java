package edu.stanford.nami.client;

import com.google.protobuf.ByteString;
import edu.stanford.nami.InTransactionGet;
import edu.stanford.nami.InTransactionPut;
import edu.stanford.nami.NKey;
import edu.stanford.nami.NamiClient;
import edu.stanford.nami.TransactionRequest;
import java.util.HashMap;
import java.util.Map;

public final class ClientTransaction {
  /** tid as of which all reads are done */
  private final long snapshotTid;

  private final NamiClient namiClient;

  private final Map<NKey, ByteString> readValues = new HashMap<>();
  private final Map<NKey, ByteString> writtenValues = new HashMap<>();

  private ClientTransaction(NamiClient namiClient, long snapshotTid) {
    this.snapshotTid = snapshotTid;
    this.namiClient = namiClient;
  }

  // get value as of snapshotTid and store the read key/value in memory so we can send it to
  // server later
  // TODO what do we do if the key doesn't exist? do we handle "null values"?
  public ByteString get(NKey key) {
    // first check if we've written it before
    if (writtenValues.containsKey(key)) {
      return writtenValues.get(key);
    } else if (readValues.containsKey(key)) {
      // we've read this value before, simply return it
      return readValues.get(key);
    } else {
      // never read/written this before, get it from the server
      // and keep track of it to include in the TransactionRequest
      var value = namiClient.get(snapshotTid, key.key());
      readValues.put(key, value);
      return value;
    }
  }

  // store the value in memory under the given key so we can send it to the server later
  // will overwrite any previous values written to this entry
  public void put(NKey key, ByteString value) {
    writtenValues.put(key, value);
  }

  // Attempt to commit this transaction to the server
  public CommitOutcome commit() {
    var txBuilder = TransactionRequest.newBuilder();
    txBuilder.setSnapshotTid(snapshotTid);
    for (var readValue : readValues.entrySet()) {
      var inTxGetBuilder =
          InTransactionGet.newBuilder()
              .setKey(readValue.getKey().key())
              .setValue(readValue.getValue());
      txBuilder.addReads(inTxGetBuilder);
    }
    for (var readValue : writtenValues.entrySet()) {
      var inTxPutBuilder =
          InTransactionPut.newBuilder()
              .setKey(readValue.getKey().key())
              .setValue(readValue.getValue());
      txBuilder.addPuts(inTxPutBuilder);
    }
    var committed = namiClient.commit(snapshotTid, readValues, writtenValues);
    // TODO actually commit the transaction to server
    return CommitOutcome.COMMITTED;
  }

  // start a new transaction against the provided Nami cluster
  public static ClientTransaction begin(NamiClient namiClient) {
    var recentTid = namiClient.getRecentTid();
    return new ClientTransaction(namiClient, recentTid);
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
