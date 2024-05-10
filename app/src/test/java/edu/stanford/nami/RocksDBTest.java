package edu.stanford.nami;

import java.nio.file.Path;
import org.junit.BeforeClass;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public abstract class RocksDBTest {

  @TempDir private Path tempDir;

  @BeforeClass
  public void loadLibrary() {
    RocksDB.loadLibrary();
  }

  public RocksDB newTransientDB() throws RocksDBException {
    try (final Options options = new Options()) {
      options.setCreateIfMissing(true);
      return RocksDB.open(options, tempDir.toAbsolutePath().toString());
    }
  }
}
