package edu.stanford.nami;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

record NVKey(long tid, String key) {

  public NVKey(long tid, String key) {
    Preconditions.checkArgument(tid > 0, "tid must be positive");
    NKey.checkIsValidNKey(key);
    this.tid = tid;
    this.key = key;
  }

  public byte[] toBytes() {
    // 1 for key length, n for key bytes, 8 for tid
    var buffer = ByteBuffer.allocate(1 + key.length() + 8);
    // first copy key length, safe b/c key.length() < Byte.MAX_VALUE
    buffer.put((byte) key.length());
    // copy string bytes, safe b/c they're all ascii
    for (int i = 0; i < key.length(); i++) {
      buffer.put((byte) key.charAt(i));
    }
    // copy tid bytes
    buffer.putLong(tid);

    return buffer.array();
  }

  public static NVKey fromBytes(byte[] bytes) {
    checkWellFormedKey(bytes.length > 1, "Bytes cannot be empty");
    var buffer = ByteBuffer.wrap(bytes);
    // key length first
    var length = buffer.get();
    checkWellFormedKey(length > 0, "Stored length must be positive");
    var expectedLength = 1 + length + 8;
    checkWellFormedKey(bytes.length == expectedLength, "Expected bytes to be of size " + expectedLength + " including header and footer");

    // safe to read the string key itself
    var key = StandardCharsets.UTF_8.decode(buffer.slice(1, length)).toString();
    // skip to one past key length + key itself
    buffer.position(1 + length); 
    var tid = buffer.getLong();

    // paranoia: check we read everything
    Preconditions.checkState(!buffer.hasRemaining(), "should have read the whole buffer");

    return new NVKey(tid, key);
  }

  private static void checkWellFormedKey(boolean condition, String msg) {
    if (condition == false) {
      throw new MalformedVKeyException(msg);
    }
  }

  public static class MalformedVKeyException extends IllegalArgumentException {
    public MalformedVKeyException(String message) {
      super(message);
    }

    public MalformedVKeyException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
