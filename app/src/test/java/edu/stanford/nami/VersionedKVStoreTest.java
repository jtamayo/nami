package edu.stanford.nami;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.primitives.Longs;
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
      var actual = new String(vkv.getExactlyAtVersion(key));

      assertThat(actual).isEqualTo(expected);
    }
  }

  @Test
  void testGetVersioned_happyPath() throws RocksDBException {
    try (RocksDB db = newTransientDB()) {
      var key = "abcd";
      var nkey = new NKey(key);
      var vkv = new VersionedKVStore(db);

      // a get on an empty db returns null
      var emptyResult = vkv.getLatest(nkey);
      assertThat(emptyResult).isNull();

      // write first version
      vkv.put(new NVKey(1, key), "one".getBytes());
      var one = vkv.getLatest(nkey);
      assertThat(new String(one)).isEqualTo("one");

      // write new version, validate we get that one
      vkv.put(new NVKey(5, key), "five".getBytes());
      var five = vkv.getLatest(nkey);
      assertThat(new String(five)).isEqualTo("five");

      // make sure we can still get the first one
      var anotherOne = vkv.getExactlyAtVersion(new NVKey(1, key));
      assertThat(new String(anotherOne)).isEqualTo("one");

      // if we ask for the wrong key, even if it's the same prefix, return null
      var notThere = vkv.getLatest(new NKey("a"));
      assertThat(notThere).isNull();
    }
  }

  @Test
  void testGetLatest_notFoundKeys() throws RocksDBException {
    try (RocksDB db = newTransientDB()) {
      var bbb = "bbb";
      var evilTidString = "bbbbbbbb";
      var evilTid = Longs.fromByteArray(evilTidString.getBytes());

      var vkv = new VersionedKVStore(db);

      // write a single value, with tid made up of a bunch of 'a's
      vkv.put(new NVKey(evilTid, bbb), "one".getBytes());

      // common prefix shouldn't find it
      assertThat(vkv.getLatest(new NKey("bb"))).isNull();

      // longer key shouldn't find it
      assertThat(vkv.getLatest(new NKey("bbbb"))).isNull();

      // key + tid shouldn't find it
      assertThat(vkv.getLatest(new NKey(bbb + evilTidString))).isNull();

      // bigger key shouldn't find it
      assertThat(vkv.getLatest(new NKey("c"))).isNull();

      // smaller key shouldn't find it
      assertThat(vkv.getLatest(new NKey("a"))).isNull();
    }
  }

  @Test
  void testGetAsOf_happyPath() throws RocksDBException {
    try (RocksDB db = newTransientDB()) {
      var key = "abcd";
      var nkey = new NKey(key);
      var vkv = new VersionedKVStore(db);
      byte[] value;

      // a get on an empty db returns null
      var emptyResult = vkv.getLatest(nkey);
      assertThat(emptyResult).isNull();

      // write first version
      vkv.put(new NVKey(1, key), "one".getBytes());

      // check we can get that version exactly
      value = vkv.getAsOf(nkey, 1);
      assertThat(new String(value)).isEqualTo("one");

      // check we can get it one past that version too
      // TODO this is strictly wrong, what if the KV store hasn't seen that version yet?
      value = vkv.getAsOf(nkey, 2);
      assertThat(new String(value)).isEqualTo("one");

      // write version 5
      vkv.put(new NVKey(5, key), "five".getBytes());

      // check we can get exactly version 5
      value = vkv.getAsOf(nkey, 5);
      assertThat(new String(value)).isEqualTo("five");

      // check we can get it past version 5 too
      value = vkv.getAsOf(nkey, 8);
      assertThat(new String(value)).isEqualTo("five");

      // if we ask for 4, we should get back 1
      value = vkv.getAsOf(nkey, 4);
      assertThat(new String(value)).isEqualTo("one");

      // if we ask for 1, we should get back 1
      value = vkv.getAsOf(nkey, 1);
      assertThat(new String(value)).isEqualTo("one");
    }
  }
}
