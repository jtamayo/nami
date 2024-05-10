package edu.stanford.nami;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class VersionedKVStoreTest extends RocksDBTest {
  @Test
  void testHappyPutGet() throws RocksDBException {
    try (RocksDB db = newTransientDB()) {
      String expected = "hello world";
      var key = new NVKey(123, "abc");
      var values = expected.getBytes();
      var vkv = new VersionedKVStore(db);

      // put the values in the store
      vkv.put(key, values);
      // retrieve them, compare the string
      var actual = new String(vkv.get(key));

      assertThat(actual).isEqualTo(expected);
    }
  }
}
