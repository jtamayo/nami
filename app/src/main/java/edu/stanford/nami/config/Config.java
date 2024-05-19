package edu.stanford.nami.config;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import java.io.File;
import java.io.IOException;

public class Config {

  public static <T> T loadConfig(File configFile, String path, Class<T> clazz) {
    var file = configFile.getParentFile().toPath().resolve(path).toFile();
    try (var reader = Files.newReader(file, Charsets.UTF_8)) {
      return new Gson().fromJson(reader, clazz);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
