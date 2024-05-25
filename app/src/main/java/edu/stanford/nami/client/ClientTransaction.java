package edu.stanford.nami.client;

import com.google.protobuf.ByteString;
import edu.stanford.nami.*;
import lombok.extern.flogger.Flogger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Flogger
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

  /**
   * Get value as of snapshotTid and store the read key/value in memory so we can send it to server
   * later
   */
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
  public TransactionResponse commit() {
    return namiClient.commit(snapshotTid, readValues, writtenValues);
  }

  // start a new transaction against the provided Nami cluster
  public static ClientTransaction begin(NamiClient namiClient, Optional<Long> snapshotTid) {
    long clientSnapshotId = snapshotTid.orElse(0L);
    var recentTid = namiClient.getRecentTid();
    var snapshotId = Math.max(recentTid, clientSnapshotId);
    if (recentTid < clientSnapshotId) {
      log.atInfo().log(
          "Got a recent id that is smaller than the one provided by client: "
              + recentTid
              + " vs. "
              + clientSnapshotId);
    }
    return new ClientTransaction(namiClient, snapshotId);
  }
}
