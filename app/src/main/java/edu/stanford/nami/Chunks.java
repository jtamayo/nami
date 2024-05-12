package edu.stanford.nami;

import java.util.List;

import lombok.Data;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class Chunks {
  @Data
  public static final class ServerAllocation {
    private String peerId;
    private List<ChunkRange> ranges;
  }

  @Data
  public static final class ChunkRange {
    private int min;
    private int max;
  }

  public Chunks.ServerAllocation loadServerAllocation(String path) {
    var file = new File(path);
    try (var reader = Files.newReader(file, Charsets.UTF_8)) {
      return gson.fromJson(reader, Chunks.ServerAllocation.class);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
