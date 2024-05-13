package edu.stanford.nami.config;

import lombok.Data;

@Data
public final class ServerConfig {
  /** Name of this server */
  private String selfPeerId;

  /** Path to the chunk configuration path */
  private String chunkConfigPath;

  /** Path to the IP addresses of all peers in the cluster */
  private String peerConfigsPath;

  /** Path to the folder that'll contain all data */
  private String dataPath;

  /** TODO remove this, it's just a bridge with the old config */
  private Integer peerIndex;
}
