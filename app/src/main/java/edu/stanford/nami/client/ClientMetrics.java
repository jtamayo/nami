package edu.stanford.nami.client;

import com.codahale.metrics.*;
import java.io.File;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j // must use Slf4j to be compatible with Slf4jReporter
public class ClientMetrics {
  public static final long METRIC_REPORTING_PERIOD_SEC = 2;

  public static final MetricRegistry registry = new MetricRegistry();
  private static CsvReporter csvReporter = null;

  public void startReporting(File metricsDirectory) {
    startLogReporting();
    startCsvReporting(metricsDirectory);
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

  public void startCsvReporting(File metricsDirectory) {
    log.atInfo().log("Logging metrics to " + metricsDirectory.toPath().toAbsolutePath());
    metricsDirectory.mkdirs();
    csvReporter =
        CsvReporter.forRegistry(registry)
            .formatFor(Locale.US)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build(metricsDirectory);
    csvReporter.start(METRIC_REPORTING_PERIOD_SEC, TimeUnit.SECONDS);
  }

  public void awaitOneLastReport() {
    try {
      csvReporter.report();
      Thread.sleep(Duration.of(METRIC_REPORTING_PERIOD_SEC, ChronoUnit.SECONDS));
    } catch (InterruptedException e) {
      // log to stderr since we're probably shutting down
      System.err.println("Interrupted while waiting for metrics to flush");
      e.printStackTrace(System.err);
    }
  }
}
