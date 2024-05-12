package edu.stanford.nami.config;

import java.util.List;

import edu.stanford.nami.Chunks;
import lombok.Data;

@Data
public class ChunksConfig {
  List<Chunks.PeerAllocation> peerAllocations;
}
