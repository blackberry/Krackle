/**
 * Copyright 2014 BlackBerry, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blackberry.kafka.lowoverhead;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

/**
 * A singleton to hold a metric registry.
 */
public class MetricRegistrySingleton {
  private static final Logger LOG = LoggerFactory
      .getLogger(MetricRegistrySingleton.class);

  private MetricRegistry metricRegistry;
  private JmxReporter jmxReporter;
  private ConsoleReporter consoleReporter;

  private MetricRegistrySingleton() {
    LOG.info("Creating metric registry");
    metricRegistry = new MetricRegistry();
  }

  private static class SingletonHolder {
    private static final MetricRegistrySingleton INSTANCE = new MetricRegistrySingleton();
  }

  /**
   * Get the instance of MetricRegistrySingleton.
   *
   * @return the instance of the singleton.
   */
  public static MetricRegistrySingleton getInstance() {
    return SingletonHolder.INSTANCE;
  }

  /**
   * Get the MetricRegistry instance associated with the singleton.
   *
   * @return the MetricRegistry
   */
  public MetricRegistry getMetricsRegistry() {
    return metricRegistry;
  }

  /**
   * Turn on JMX reporting for this MetricRegistry.
   *
   * It is safe to call this multiple times.
   */
  public synchronized void enableJmx() {
    if (jmxReporter == null) {
      LOG.info("Starting JMX reporting");
      jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
      jmxReporter.start();
    }
  }

  /**
   * Enable console reporting for this MetricRegistry with 1 minute frequency.
   */
  public void enableConsole() {
    enableConsole(60000);
  }

  /**
   * Enable console reporting for this MetricRegistry.
   *
   * @param ms
   *          how often to output data to the console, in milliseconds.
   */
  public synchronized void enableConsole(long ms) {
    if (consoleReporter == null) {
      LOG.info("Starting console reporting with frequency {}ms", ms);

      consoleReporter = ConsoleReporter.forRegistry(metricRegistry)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS).build();
      consoleReporter.start(ms, TimeUnit.MILLISECONDS);
    }
  }
}
