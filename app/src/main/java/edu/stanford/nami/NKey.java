package edu.stanford.nami;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;

/**
 * An un-versioned key in nami. Keys in nami are simply strings of printable ASCII characters that
 * encode/decode themselves as bytes.
 */
public record NKey(String key) {
  public static final char MIN_PRINTABLE_CHAR = '!';
  public static final char MAX_PRINTABLE_CHAR = '~';

  public NKey(String key) {
    checkIsValidNKey(key);
    this.key = key;
  }

  public static int computeByteSize(String key) {
    return 1 + key.length();
  }

  public static void writeToBuffer(String key, ByteBuffer buffer) {
    // first copy key length, safe b/c key.length() < Byte.MAX_VALUE
    buffer.put((byte) key.length());
    // copy string bytes, safe b/c they're all ascii
    for (int i = 0; i < key.length(); i++) {
      buffer.put((byte) key.charAt(i));
    }
  }

  public static boolean isPrintable(String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c < MIN_PRINTABLE_CHAR || c > MAX_PRINTABLE_CHAR) return false;
    }
    return true;
  }

  public static void checkIsValidNKey(String key) throws IllegalArgumentException {
    Preconditions.checkArgument(!key.isEmpty(), "Keys cannot be empty");
    Preconditions.checkArgument(
        key.length() < Byte.MAX_VALUE, "keys must be at most " + Byte.MAX_VALUE + " in length");
    Preconditions.checkArgument(isPrintable(key), "keys must be only ascii printable characters");
  }
}
