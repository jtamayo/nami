package edu.stanford.nami;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import edu.stanford.nami.Chunks.ChunkRange;
import edu.stanford.nami.Chunks.PeerAllocation;
import edu.stanford.nami.config.ChunksConfig;
import edu.stanford.nami.config.PeersConfig;
import edu.stanford.nami.config.PeersConfig.PeerConfig;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.flogger.Flogger;

@Flogger
public class RemoteStore implements AutoCloseable {
  private final Chunks.KeyToChunkMapper keyToChunkMapper = Chunks.NaiveKeyToChunkMapper.INSTANCE;
  private final String selfId;
  private final ChunksConfig chunksConfig;
  private final Map<String, KVStoreGrpc.KVStoreBlockingStub> peerClients;
  private final Map<String, KVStoreGrpc.KVStoreFutureStub> asyncPeerClients;
  private final List<String> peerNames;
  private final List<ManagedChannel> peerChannels;

  public RemoteStore(String selfId, PeersConfig peersConfig, ChunksConfig chunksConfig) {
    Preconditions.checkNotNull(selfId);
    Preconditions.checkNotNull(peersConfig);
    Preconditions.checkNotNull(chunksConfig);

    var peerClientsBuilder = ImmutableMap.<String, KVStoreGrpc.KVStoreBlockingStub>builder();
    var asyncPeerClientsBuilder = ImmutableMap.<String, KVStoreGrpc.KVStoreFutureStub>builder();
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
      var asyncPeerClient = KVStoreGrpc.newFutureStub(channel);
      peerClientsBuilder.put(peerId, peerClient);
      peerChannelsBuilder.add(channel);
      asyncPeerClientsBuilder.put(peerId, asyncPeerClient);
      log.atFine().log("Opening channel to %s", target);
    }
    this.peerClients = peerClientsBuilder.build();
    this.asyncPeerClients = asyncPeerClientsBuilder.build();
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

  public Map<NKey, ByteString> getAsOf(Set<NKey> keys, long tid) {
    var peersToKeys = findPeersWithKeys(keys);
    // copy the peers to a list, so we can keep track of them in order
    // peersToKeys is an ArrayListMultimap, so it'll keep each key in order too
    var orderedPeers = ImmutableList.copyOf(peersToKeys.keySet());
    var responseFutures = new ArrayList<ListenableFuture<GetBatchResponse>>(peerClients.size());
    for (var peer : orderedPeers) {
      var keysInPeer = peersToKeys.get(peer);
      var getBatchRequestBuilder = GetBatchRequest.newBuilder();
      for (var key : keysInPeer) {
        var protoVKey = ProtoVKey.newBuilder().setTid(tid).setKey(key.key()).build();
        getBatchRequestBuilder.addKeys(protoVKey);
      }
      var asyncPeerClient = asyncPeerClients.get(peer);
      var response = asyncPeerClient.getBatch(getBatchRequestBuilder.build());
      responseFutures.add(response);
    }
    var allResponsesFuture = Futures.allAsList(responseFutures);
    try {
      var responses = allResponsesFuture.get();
      // sanity check: same number of results as requests
      Preconditions.checkState(responses.size() == orderedPeers.size());
      var result = new HashMap<NKey, ByteString>(keys.size());
      for (int i = 0; i < responses.size(); i++) {
        var peer = orderedPeers.get(i);
        var keysInPeer = peersToKeys.get(peer);
        var values = responses.get(i).getValuesList();
        Preconditions.checkState(keysInPeer.size() == values.size());
        for (int j = 0; j < values.size(); j++) {
          var nKey = keysInPeer.get(j);
          var value = values.get(j);
          result.put(nKey, value);
        }
      }
      return result;
    } catch (Exception e) {
      // TODO need to distinguish between transient/permanent errors, and retry with different
      // backends
      log.atSevere().log("Failed to getAsOf for keys %s and tid %s", keys, tid, e);
      throw new RuntimeException(e);
    }
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

  /**
   * Find a small-ish set of peers that can serve all the requested keys. This is done by picking
   * peers at random, and getting as many keys as possible from each one.
   *
   * <p>Probably way too CPU intensive, but it might be better than one extra RPC, and it can easily
   * accommodate retries if one peer is down.
   */
  private ArrayListMultimap<String, NKey> findPeersWithKeys(Set<NKey> keys) {
    // shuffle peers
    var shuffledPeerAllocations = new ArrayList<>(chunksConfig.getPeerAllocations());
    Collections.shuffle(shuffledPeerAllocations);
    // now go through each peer in (random) order, try to get as many keys as possible
    var pendingKeys = new HashSet<>(keys);
    var peerToKeys = ArrayListMultimap.<String, NKey>create(keys.size() / 2, 2);
    for (var allocation : shuffledPeerAllocations) {
      if (allocation.peerId().equals(selfId)) {
        // skip yourself
        continue;
      }
      for (var pendingKeysIterator = pendingKeys.iterator(); pendingKeysIterator.hasNext(); ) {
        var nKey = pendingKeysIterator.next();
        var keyChunk = keyToChunkMapper.map(nKey);
        for (ChunkRange range : allocation.ranges()) {
          if (range.min() <= keyChunk && keyChunk <= range.max()) {
            // this peer can serve this key
            peerToKeys.put(allocation.peerId(), nKey);
            // and this key is taken care of now
            pendingKeysIterator.remove();
          }
        }
      }
    }
    // validate all keys have been assigned
    Preconditions.checkState(
        pendingKeys.isEmpty(), "Some keys don't have a matching allocation: " + pendingKeys);
    return peerToKeys;
  }
}
