package edu.stanford.nami;

import java.util.List;

import com.google.common.base.Preconditions;

import lombok.Data;

@Data
public final class ServerConfig {
  private String peerId;
  private String chunkConfigPath;
  private String peerConfigsPath;
}
