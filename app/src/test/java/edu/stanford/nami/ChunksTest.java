package edu.stanford.nami;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.base.Preconditions;
import org.junit.jupiter.api.Test;

public class ChunksTest {
  @Test
  void testNaiveKeyToChunkMapper() {
    var mapper = new Chunks.NaiveKeyToChunkMapper();
    // check for one byte min/max printable characters
    assertThat(mapper.map(nKey(0x21))).isEqualTo(0x2100);
    assertThat(mapper.map(nKey(0x7E))).isEqualTo(0x7E00);

    // check for 2 byte strings
    assertThat(mapper.map(nKey(0x21, 0x21))).isEqualTo(0x2121);
    assertThat(mapper.map(nKey(0x21, 0x7E))).isEqualTo(0x217E);
    assertThat(mapper.map(nKey(0x7E, 0x21))).isEqualTo(0x7E21);
    assertThat(mapper.map(nKey(0x7E, 0x7E))).isEqualTo(0x7E7E);
  }

  NKey nKey(int first) {
    Preconditions.checkArgument(first > 0 && first < Byte.MAX_VALUE);
    return new NKey(new String(new char[] {(char) first}));
  }

  NKey nKey(int first, int second) {
    Preconditions.checkArgument(first > 0 && first < Byte.MAX_VALUE);
    Preconditions.checkArgument(second > 0 && second < Byte.MAX_VALUE);
    return new NKey(new String(new char[] {(char) first, (char) second}));
  }
}
