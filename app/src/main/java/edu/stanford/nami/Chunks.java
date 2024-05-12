package edu.stanford.nami;

import java.io.File;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import lombok.Data;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class Chunks {
  @Data
  public static final class PeerAllocation {
    private String peerId;
    private List<ChunkRange> ranges;
  }

  @Data
  public static final class ChunkRange {
    private int min;
    private int max;
  }
}
