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
}
