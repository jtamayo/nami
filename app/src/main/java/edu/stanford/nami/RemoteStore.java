package edu.stanford.nami;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import edu.stanford.nami.Chunks.ChunkRange;
import edu.stanford.nami.Chunks.PeerAllocation;
import edu.stanford.nami.config.ChunksConfig;
import edu.stanford.nami.config.PeersConfig;
import edu.stanford.nami.config.PeersConfig.PeerConfig;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import java.nio.ByteBuffer;
import java.util.Map;

public class RemoteStore {
  private final Chunks.KeyToChunkMapper keyToChunkMapper = Chunks.NaiveKeyToChunkMapper.INSTANCE;
  private final String selfId;
  private final ChunksConfig chunksConfig;
  private final Map<String, KVStoreGrpc.KVStoreBlockingStub> peerClients;

  public RemoteStore(String selfId, PeersConfig peersConfig, ChunksConfig chunksConfig) {
    Preconditions.checkNotNull(selfId);
    Preconditions.checkNotNull(peersConfig);
    Preconditions.checkNotNull(chunksConfig);

    var peerClientsBuilder = ImmutableMap.<String, KVStoreGrpc.KVStoreBlockingStub>builder();
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
    var peerGrpc = findPeerWithKey(key.nKey());
    ProtoVKey protoVKey = ProtoVKey.newBuilder().setTid(key.tid()).setKey(key.key()).build();
    GetRequest request = GetRequest.newBuilder().setKey(protoVKey).build();
    GetResponse response = peerGrpc.get(request);
    return response.getValue().asReadOnlyByteBuffer();
  }

  private KVStoreGrpc.KVStoreBlockingStub findPeerWithKey(NKey key) {
    var keyChunk = keyToChunkMapper.map(key);
    // most naive impl possible: go through chunks, pick first one that has it
    for (PeerAllocation allocation : chunksConfig.getPeerAllocations()) {
      if (allocation.peerId().equals(selfId)) {
        // skip yourself
        continue;
      }
      for (ChunkRange range : allocation.ranges()) {
        if (range.min() <= keyChunk && keyChunk <= range.max()) {
          // found a peer that works
          return peerClients.get(allocation.peerId());
        }
      }
    }
    throw new IllegalStateException("There is no allocation that matches key " + key);
  }
}
