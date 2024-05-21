package edu.stanford.nami.utils;

import java.util.Arrays;
import lombok.experimental.UtilityClass;

@UtilityClass
public class AutoCloseables {
  public void closeSafely(AutoCloseable... all) throws Exception {
    closeSafely(Arrays.asList(all));
  }

  public void closeSafely(Iterable<AutoCloseable> all) throws Exception {
    Exception first = null;
    for (var closeable : all) {
      try {
        closeable.close();
      } catch (Exception e) {
        if (first != null) {
          first.addSuppressed(e);
        } else {
          first = e;
        }
      }
    }
    if (first != null) {
      throw first;
    }
  }
}
