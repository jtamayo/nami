package edu.stanford.nami.config;

import lombok.Data;

@Data
public final class ClientConfig {
  /** Path to the chunk configuration path */
  private String chunkConfigPath;

  /** Path to the IP addresses of all peers in the cluster */
  private String peerConfigsPath;

  /** Path to where metrics will be logged */
  private String metricsPath;
}
