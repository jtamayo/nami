package edu.stanford.nami.client;

import com.google.protobuf.ByteString;
import edu.stanford.nami.NKey;
import edu.stanford.nami.TransactionResponse;

public interface ClientTransaction {
  ByteString get(NKey key);

  void put(NKey key, ByteString value);

  TransactionResponse commit();
}
