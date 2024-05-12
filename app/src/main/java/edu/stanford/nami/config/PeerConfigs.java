package edu.stanford.nami.config;

import java.util.List;

import lombok.Data;

/**
 * Connection information for all peers in the cluster.
 */
@Data
public class PeerConfigs {
  List<PeerConfig> peerConfigs;

  @Data
  public static class PeerConfig {
    /** 
     * Unique peer identifier. Name must match across chunk configs and raft configs.
     */
    String peerId;
    /** IP address/name of where the server is located, i.e. localhost, or 10.2.3.4. */
    String address;
    /** Port where the raft server is listening. */
    int raftPort;
    /** Port where the nami GRPC server is listening. */
    int kvPort;
  }
}
