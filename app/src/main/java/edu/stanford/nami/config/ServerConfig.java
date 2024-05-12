package edu.stanford.nami.config;

import java.util.List;

import com.google.common.base.Preconditions;

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
