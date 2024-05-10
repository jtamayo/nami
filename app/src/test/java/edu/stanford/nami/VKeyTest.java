package edu.stanford.nami;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Test;

public class VKeyTest {
  @Test
  void testHappySerde() {
    var a = new NVKey(123, "abc");
    var bytes = a.toBytes();
    var b = NVKey.fromBytes(bytes);

    assertThat(a).isEqualTo(b);
    assertThat(bytes.length).isEqualTo(12);
  }
}
