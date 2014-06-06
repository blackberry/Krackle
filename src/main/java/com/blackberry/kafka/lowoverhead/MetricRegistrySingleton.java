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
