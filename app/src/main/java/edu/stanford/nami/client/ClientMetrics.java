package edu.stanford.nami.client;

import com.codahale.metrics.*;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j // must use Slf4j to be compatible with Slf4jReporter
public class ClientMetrics {
  public static final long METRIC_REPORTING_PERIOD_SEC = 5;

  public static final MetricRegistry registry = new MetricRegistry();

  public void startReporting(String csvPath) {
    startLogReporting();
    startCsvReporting(csvPath);
  }

  public void startLogReporting() {
    Slf4jReporter reporter =
        Slf4jReporter.forRegistry(registry)
            .outputTo(log)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
    reporter.start(METRIC_REPORTING_PERIOD_SEC, TimeUnit.SECONDS);
  }

  public void startCsvReporting(String path) {
    var outputFile = new File(path);
    log.atInfo().log("Logging metrics to " + path);
    outputFile.mkdirs();
    CsvReporter reporter =
        CsvReporter.forRegistry(registry)
            .formatFor(Locale.US)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build(outputFile);
    reporter.start(1, TimeUnit.SECONDS);
  }

  public void awaitOneLastReport() {
    try {
      Thread.sleep(Duration.of(METRIC_REPORTING_PERIOD_SEC, ChronoUnit.SECONDS));
    } catch (InterruptedException e) {
      // log to stderr since we're probably shutting down
      System.err.println("Interrupted while waiting for metrics to flush");
      e.printStackTrace(System.err);
    }
  }
}
