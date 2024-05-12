package edu.stanford.nami.config;

import edu.stanford.nami.Chunks;
import java.util.List;
import lombok.Data;

@Data
public class ChunksConfig {
  List<Chunks.PeerAllocation> peerAllocations;
}
