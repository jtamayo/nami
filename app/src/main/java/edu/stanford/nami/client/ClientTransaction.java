package edu.stanford.nami.client;

import com.google.protobuf.ByteString;
import edu.stanford.nami.*;
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
  public TransactionStatus commit() {
    var response = namiClient.commit(snapshotTid, readValues, writtenValues);
    return response.getStatus();
  }

  // start a new transaction against the provided Nami cluster
  public static ClientTransaction begin(NamiClient namiClient) {
    var recentTid = namiClient.getRecentTid();
    return new ClientTransaction(namiClient, recentTid);
  }
}
