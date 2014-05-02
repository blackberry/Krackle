package com.blackberry.kafka.lowoverhead.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class Metrics {

  private MetricRegistry metrics;
  private JmxReporter jmxReporter;
  private ConsoleReporter consoleReporter;

  private Meter receivedTotal;
  private Map<String, Meter> receivedByTopic = new HashMap<String, Meter>();

  private Meter sentTotal;
  private Map<String, Meter> sentByTopic = new HashMap<String, Meter>();

  private Meter droppedTotal;
  private Map<String, Meter> droppedByTopic = new HashMap<String, Meter>();

  private Metrics() {
  }

  private static class SingletonHolder {
    private static final Metrics INSTANCE = new Metrics();
  }

  public static Metrics getInstance() {
    return SingletonHolder.INSTANCE;
  }

  public void configure(ProducerConfiguration conf) {
    metrics = new MetricRegistry();

    jmxReporter = JmxReporter.forRegistry(metrics).build();
    jmxReporter.start();

    if (conf.isMetricsToConsole()) {
      consoleReporter = ConsoleReporter.forRegistry(metrics)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS).build();
      consoleReporter.start(conf.getMetricsToConsoleIntervalMs(),
          TimeUnit.MILLISECONDS);
    }

    receivedTotal = metrics.meter(MetricRegistry.name(
        LowOverheadProducer.class, "total messages received"));
    sentTotal = metrics.meter(MetricRegistry.name(LowOverheadProducer.class,
        "total messages sent"));
    droppedTotal = metrics.meter(MetricRegistry.name(LowOverheadProducer.class,
        "total messages dropped"));
  }

  Meter m;

  public synchronized void addTopic(String topic) {
    m = receivedByTopic.get(topic);
    if (m == null) {
      m = metrics.meter(MetricRegistry.name(LowOverheadProducer.class,
          "messages received : " + topic));
      receivedByTopic.put(topic, m);
    }

    m = sentByTopic.get(topic);
    if (m == null) {
      m = metrics.meter(MetricRegistry.name(LowOverheadProducer.class,
          "messages sent : " + topic));
      sentByTopic.put(topic, m);
    }

    m = droppedByTopic.get(topic);
    if (m == null) {
      m = metrics.meter(MetricRegistry.name(LowOverheadProducer.class,
          "messages dropped : " + topic));
      droppedByTopic.put(topic, m);
    }
  }

  public void receivedMark() {
    receivedTotal.mark();
  }

  public void receivedMark(long n) {
    receivedTotal.mark(n);
  }

  public void receivedMark(String topic) {
    receivedByTopic.get(topic).mark();
    receivedTotal.mark();
  }

  public void receivedMark(String topic, long n) {
    receivedByTopic.get(topic).mark(n);
    receivedTotal.mark(n);
  }

  public void sentMark() {
    sentTotal.mark();
  }

  public void sentMark(long n) {
    sentTotal.mark(n);
  }

  public void sentMark(String topic) {
    sentByTopic.get(topic).mark();
    sentTotal.mark();
  }

  public void sentMark(String topic, long n) {
    sentByTopic.get(topic).mark(n);
    sentTotal.mark(n);
  }

  public void droppedMark() {
    droppedTotal.mark();
  }

  public void droppedMark(long n) {
    droppedTotal.mark(n);
  }

  public void droppedMark(String topic) {
    droppedByTopic.get(topic).mark();
    droppedTotal.mark();
  }

  public void droppedMark(String topic, long n) {
    droppedByTopic.get(topic).mark(n);
    droppedTotal.mark(n);
  }
}
