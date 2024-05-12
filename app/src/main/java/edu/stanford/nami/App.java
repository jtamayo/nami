/*
 * This source file was generated by the Gradle 'init' task
 */
package edu.stanford.nami;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import edu.stanford.nami.config.ChunksConfig;
import edu.stanford.nami.config.PeersConfig;
import edu.stanford.nami.config.ServerConfig;
import java.io.File;
import java.io.IOException;

public class App {

  public String getGreeting() {
    return "Hello World!";
  }

  public static void main(String[] args) {
    System.out.println("Running in " + (new File(".").getAbsolutePath()));

    if (args.length != 1) {
      System.err.println("Invalid usage. Usage: nami <config_file>");
      System.exit(-1);
    }
    var configFileName = args[0];
    var configFile = new File(configFileName);

    if (!configFile.exists()) {
      System.err.println("File " + configFile.getAbsolutePath() + " does not exist");
      System.exit(-2);
    } else {
      System.out.println("Found config file at " + configFile.getAbsolutePath());
    }

    var config = loadServerConfig(configFile);
    System.out.println("Loaded server config " + config);
    var serverAllocation = loadPeersConfig(configFile, config.getPeerConfigsPath());
    System.out.println(serverAllocation);
    var chunksConfig = loadChunksConfig(configFile, config.getChunkConfigPath());
    System.out.println(chunksConfig);
  }

  private static ServerConfig loadServerConfig(File configFile) {
    try (var reader = Files.newReader(configFile, Charsets.UTF_8)) {
      return new Gson().fromJson(reader, ServerConfig.class);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static PeersConfig loadPeersConfig(File configFile, String path) {
    return loadConfig(configFile, path, PeersConfig.class);
  }

  public static ChunksConfig loadChunksConfig(File configFile, String path) {
    return loadConfig(configFile, path, ChunksConfig.class);
  }

  public static <T> T loadConfig(File configFile, String path, Class<T> clazz) {
    var file = configFile.getParentFile().toPath().resolve(path).toFile();
    try (var reader = Files.newReader(file, Charsets.UTF_8)) {
      return new Gson().fromJson(reader, clazz);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
