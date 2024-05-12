package edu.stanford.nami;

import com.google.common.base.Preconditions;
import java.util.List;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class Chunks {
  public static final int MIN_CHUNK_INDEX = 0;
  public static final int MAX_CHUNK_INDEX = Short.MAX_VALUE; // 32k

  public record PeerAllocation(String peerId, List<ChunkRange> ranges) {}

  public record ChunkRange(int min, int max) {}

  /** Assigns a chunk to a NKey. */
  public interface KeyToChunkMapper {
    public int map(NKey key);
  }

  /**
   * Simplest key to chunk assignment possible: first two bytes of key become a short, then it gets
   * bit-shifted until it fits in the chunk range.
   */
  public static final class NaiveKeyToChunkMapper implements KeyToChunkMapper {
    public static final NaiveKeyToChunkMapper INSTANCE = new NaiveKeyToChunkMapper();

    public int map(NKey key) {
      int val = ((int) key.key().charAt(0)) << 8;
      if (key.key().length() > 1) {
        val += key.key().charAt(1);
      }
      // paranoia: make sure we're in range
      Preconditions.checkState(val >= MIN_CHUNK_INDEX && val <= MAX_CHUNK_INDEX);
      return val;
    }
  }
}
