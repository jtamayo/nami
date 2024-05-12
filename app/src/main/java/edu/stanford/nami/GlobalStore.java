package edu.stanford.nami;

import java.util.Map;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.stanford.nami.Chunks.ChunkRange;
import edu.stanford.nami.Chunks.PeerAllocation;
import edu.stanford.nami.KVStoreGrpc.KVStoreBlockingStub;
import edu.stanford.nami.config.ChunksConfig;
import edu.stanford.nami.config.PeersConfig;
import edu.stanford.nami.config.PeersConfig.PeerConfig;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

/**
 * Provides a single unified view of all the VersionedKVStores in the system.
 * 
 * To do so it needs:
 * - The local stores
 * - The remote stores
 * - The cache
 * 
 * The next question is who's doing the writes? this guy? someone else? 
 * 
 * Timing wise we also get into tricky issues with previous keys and such, so yeah. Kind of awkward.
 * 
 * So assume this guy is just reading. Let's focus on that. 
 * 
 * To read, it first hits the local store. If not there, then the cache. If not there,
 * it needs to determine which server has the data, and fetch from it.
 * 
 * So then each server needs a set of server clients, one for each peer. Then you map
 * from get to chunk, then from chunk to server, then you call the get, then you return.
 * 
 * Design decisions, I guess I should build this ok from scratch
 * So each peer has a set of ranges it can serve
 * then we have a routing algo
 * then we need to get a bunch of keys
 */
public class GlobalStore {
  private final String selfId;
  private final ChunksConfig chunksConfig;
  private final Map<String, KVStoreGrpc.KVStoreBlockingStub> peerClients;

  public GlobalStore(String selfId, PeersConfig peersConfig, ChunksConfig chunksConfig) {
    Preconditions.checkNotNull(selfId);
    Preconditions.checkNotNull(peersConfig);
    Preconditions.checkNotNull(chunksConfig);

    var peerClientsBuilder = ImmutableMap.<String, KVStoreGrpc.KVStoreBlockingStub> builder();
    // first go through all peers, and as long as it's not "self", create a KVStoreClient
    for (PeerConfig peerConfig : peersConfig.getPeers()) {
      var peerId = peerConfig.getPeerId();
      if (selfId.equals(peerConfig.getPeerId())) {
        // no need to construct a remote client for yourself
        continue;
      }
      var target = peerConfig.getAddress() + ":" + peerConfig.getKvPort();
      var channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
      var peerClient = KVStoreGrpc.newBlockingStub(channel);
      peerClientsBuilder.put(peerId, peerClient);
    }
    this.peerClients = peerClientsBuilder.build();
    this.chunksConfig = chunksConfig;
    this.selfId = selfId;
  }

  public ByteBuffer get(NVKey key) {
    // ProtoVKey protoVKey = ProtoVKey.newBuilder().setTid(tid).setKey(key).build();
    // GetRequest request = GetRequest.newBuilder().setKey(protoVKey).build();
    // GetResponse response = blockingStub.get(request);
    // System.out.println("Got get response: " + response.toString());
    throw new RuntimeException("unimplemented");
  }

  private KVStoreGrpc.KVStoreBlockingStub findPeerWithKey(NVKey key) {
    return null;
    // // most naive impl possible: go through chunks, pick first one that has it
    // for (PeerAllocation allocation : chunksConfig.getPeerAllocations()) {
    //   if (allocation.getPeerId().equals(selfId)) {
    //     // skip yourself
    //     continue;
    //   }
    //   for (ChunkRange range : allocation.getRanges()) {
    //     if (range)
    //   }
    // }
  }

}
