package edu.stanford.nami.config;

import java.util.*;
import lombok.Data;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

/** Connection information for all peers in the cluster. */
@Data
public class PeersConfig {
  List<PeerConfig> peers;
  private static final UUID GROUP_ID = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");

  public static RaftGroupId geRaftGroupId() {
    return RaftGroupId.valueOf(GROUP_ID);
  }

  public RaftGroup getRaftGroup() {
    List<RaftPeer> raftPeers =
        peers.stream()
            .map(
                peer ->
                    RaftPeer.newBuilder()
                        .setId(peer.getPeerId())
                        .setAddress(peer.getRaftAddress())
                        .setPriority(peer.getPriority())
                        .build())
            .toList();
    return RaftGroup.valueOf(RaftGroupId.valueOf(GROUP_ID), raftPeers);
  }

  @Data
  public static class PeerConfig {
    /** Unique peer identifier. Name must match across chunk configs and raft configs. */
    String peerId;

    /** IP address/name of where the server is located, i.e. localhost, or 10.2.3.4. */
    String host;

    /** Port where the raft server is listening. */
    int raftPort;

    /** Port where the nami GRPC server is listening. */
    int kvPort;

    /**
     * Used to determine whether this peer should be the leader. The higher the number the more
     * weight.
     */
    int priority;

    public String getRaftAddress() {
      return host + ":" + raftPort;
    }

    public String getKvAddress() {
      return host + ":" + kvPort;
    }
  }
}
