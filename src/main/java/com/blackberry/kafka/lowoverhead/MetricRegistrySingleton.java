package com.blackberry.kafka.lowoverhead;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

public class MetricRegistrySingleton {

  private MetricRegistry metricRegistry;
  private JmxReporter jmxReporter;
  private ConsoleReporter consoleReporter;

  private MetricRegistrySingleton() {
    metricRegistry = new MetricRegistry();
  }

  private static class SingletonHolder {
    private static final MetricRegistrySingleton INSTANCE = new MetricRegistrySingleton();
  }

  public static MetricRegistrySingleton getInstance() {
    return SingletonHolder.INSTANCE;
  }

  public MetricRegistry getMetricsRegistry() {
    return metricRegistry;
  }

  public synchronized void enableJmx() {
    if (jmxReporter == null) {
      jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
      jmxReporter.start();
    }
  }

  public void enableConsole() {
    enableConsole(60000);
  }

  public synchronized void enableConsole(long ms) {
    if (consoleReporter == null) {
      consoleReporter = ConsoleReporter.forRegistry(metricRegistry)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS).build();
      consoleReporter.start(ms, TimeUnit.MILLISECONDS);
    }
  }
}
