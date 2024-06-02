package edu.stanford.nami.utils;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.function.*;
import lombok.experimental.UtilityClass;
import lombok.extern.flogger.Flogger;

@UtilityClass
@Flogger
public class GrpcRetries {
  public <T> T withGrpcRetries(Supplier<T> s) {
    while (true) {
      try {
        return s.get();
      } catch (StatusRuntimeException e) {
        if (!isRetryable(e.getStatus())) {
          log.atWarning().log("Non-retryable status %s, aborting", e.getStatus());
          // not retryable, just propagate up
          throw e;
        } else {
          // just continue in the while loop, which will retry with a different server
          log.atWarning().log("Error %s while getting keys, retrying...", e.getStatus());
        }
      }
    }
  }

  public static boolean isRetryable(Status status) {
    return status.getCode().equals(Status.Code.UNAVAILABLE);
  }
}
