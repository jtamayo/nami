package edu.stanford.nami;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import edu.stanford.nami.Chunks.ChunkRange;
import edu.stanford.nami.Chunks.PeerAllocation;
import edu.stanford.nami.config.ChunksConfig;
import edu.stanford.nami.config.PeersConfig;
import edu.stanford.nami.config.PeersConfig.PeerConfig;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import lombok.extern.flogger.Flogger;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Flogger
public class RemoteStore implements AutoCloseable {
  private final Chunks.KeyToChunkMapper keyToChunkMapper = Chunks.NaiveKeyToChunkMapper.INSTANCE;
  private final String selfId;
  private final ChunksConfig chunksConfig;
  private final Map<String, KVStoreGrpc.KVStoreBlockingStub> peerClients;
  private final List<String> peerNames;
  private final List<ManagedChannel> peerChannels;

  public RemoteStore(String selfId, PeersConfig peersConfig, ChunksConfig chunksConfig) {
    Preconditions.checkNotNull(selfId);
    Preconditions.checkNotNull(peersConfig);
    Preconditions.checkNotNull(chunksConfig);

    var peerClientsBuilder = ImmutableMap.<String, KVStoreGrpc.KVStoreBlockingStub>builder();
    var peerChannelsBuilder = ImmutableList.<ManagedChannel>builder();
    // first go through all peers, and as long as it's not "self", create a KVStoreClient
    for (PeerConfig peerConfig : peersConfig.getPeers()) {
      var peerId = peerConfig.getPeerId();
      if (selfId.equals(peerConfig.getPeerId())) {
        // no need to construct a remote client for yourself
        continue;
      }
      var target = peerConfig.getKvAddress();
      var channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
      var peerClient = KVStoreGrpc.newBlockingStub(channel);
      peerClientsBuilder.put(peerId, peerClient);
      peerChannelsBuilder.add(channel);
      log.atFine().log("Opening channel to %s", target);
    }
    this.peerClients = peerClientsBuilder.build();
    this.peerChannels = peerChannelsBuilder.build();
    this.peerNames = ImmutableList.copyOf(peerClients.keySet());
    this.chunksConfig = chunksConfig;
    this.selfId = selfId;
  }

  public ByteString getAsOf(NKey key, long tid) {
    var peerGrpc = findPeerWithKey(key);
    log.atFine().log("Getting key %s from peer %s", key, peerGrpc);
    ProtoVKey protoVKey = ProtoVKey.newBuilder().setTid(tid).setKey(key.key()).build();
    GetRequest request = GetRequest.newBuilder().setKey(protoVKey).build();
    GetResponse response = peerGrpc.get(request);
    return response.getValue();
  }

  /**
   * Pick a peer at random out of all the known peers. Good for balancing load when any one peer
   * could answer a query.
   */
  public KVStoreGrpc.KVStoreBlockingStub getArbitraryPeer() {
    var peerIndex = new Random().nextInt(peerNames.size());
    return peerClients.get(peerNames.get(peerIndex));
  }

  @Override
  public void close() throws Exception {
    // Not really sure is worth separating these two, but I'll just follow whatever
    // ratis does
    shutdown();
  }

  public void shutdown() throws InterruptedException {
    for (var channel : this.peerChannels) {
      log.atFine().log("Shutting down channel %s", channel);
      channel.shutdown();
    }
    boolean interrupted = false;
    for (var channel : this.peerChannels) {
      try {
        log.atFine().log("Waiting on channel %s to terminate", channel);
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        interrupted = true;
      }
    }
    if (interrupted) {
      throw new InterruptedException();
    }
    log.atFine().log("Shut down RemoteStore");
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
