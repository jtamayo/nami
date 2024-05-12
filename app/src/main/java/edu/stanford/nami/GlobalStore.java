package edu.stanford.nami;

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
 * 
 */
public class GlobalStore {
  
}
