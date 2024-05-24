package edu.stanford.nami.config;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import edu.stanford.nami.Chunks.ChunkRange;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;

public class Config {
  private static final Gson gson =
      new GsonBuilder()
          .registerTypeAdapter(ChunkRange.class, new ChunkRangeDeserializer())
          .create();

  public static <T> T loadConfig(File configFile, String path, Class<T> clazz) {
    var file = configFile.getParentFile().toPath().resolve(path).toFile();
    try (var reader = Files.newReader(file, Charsets.UTF_8)) {
      return gson.fromJson(reader, clazz);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static final class ChunkRangeDeserializer implements JsonDeserializer<ChunkRange> {

    @Override
    public ChunkRange deserialize(
        JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      var jsonArray = json.getAsJsonArray();
      Preconditions.checkArgument(jsonArray.size() == 2, "Chunk must be [min, max]");
      return new ChunkRange(jsonArray.get(0).getAsInt(), jsonArray.get(1).getAsInt());
    }
  }
}
