package com.blackberry.kafka.lowoverhead;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.blackberry.kafka.lowoverhead.consumer.ConsumerConfiguration;
import com.blackberry.kafka.lowoverhead.consumer.LowOverheadConsumer;
import com.blackberry.kafka.lowoverhead.meta.MetaData;
import com.blackberry.kafka.lowoverhead.producer.LowOverheadProducer;
import com.blackberry.kafka.lowoverhead.producer.ProducerConfiguration;
import com.blackberry.testutil.LocalKafkaServer;
import com.blackberry.testutil.LocalZkServer;

public class KafkaClientTest {
    private static final String[] COMPRESSION_METHODS = new String[] { "none",
	    "snappy", "gzip" };

    Throwable error = null;

    static LocalZkServer zk;
    static LocalKafkaServer kafka;

    static List<String> logs;

    @BeforeClass
    public static void setup() throws Exception {
	zk = new LocalZkServer();
	kafka = new LocalKafkaServer();

	logs = new ArrayList<String>();
	for (int i = 0; i < 1000; i++) {
	    logs.add("This is a test log line.  Number " + i);
	}
    }

    @AfterClass
    public static void cleanup() throws Exception {
	kafka.shutdown();
	zk.shutdown();
    }

    private void setupTopic(String topic) throws Exception {
	kafka.createTopic(topic);

	// Wait for everything to finish starting up. We do this by checking to
	// ensure all the topics have leaders.
	Properties producerProps = new Properties();
	producerProps.setProperty("metadata.broker.list", "localhost:9876");
	ProducerConfiguration producerConf = new ProducerConfiguration(
		producerProps);
	while (true) {
	    MetaData meta;
	    try {
		meta = MetaData.getMetaData(
			producerConf.getMetadataBrokerList(), topic, "test");
		meta.getTopic(topic).getPartition(0).getLeader();
		break;
	    } catch (Exception e) {
		// System.err.print("Not ready yet: ");
		// e.printStackTrace();
	    } finally {
		Thread.sleep(100);
	    }
	}
    }

    private Producer<String, String> getStdProducer(String compression) {
	Properties producerProps = new Properties();
	producerProps.setProperty("metadata.broker.list", "localhost:9876");
	producerProps.setProperty("request.required.acks", "1");
	producerProps.setProperty("producer.type", "async");
	producerProps.setProperty("serializer.class",
		"kafka.serializer.StringEncoder");
	producerProps.setProperty("compression.codec", compression);
	ProducerConfig producerConf = new ProducerConfig(producerProps);
	Producer<String, String> producer = new Producer<String, String>(
		producerConf);
	return producer;
    }

    private LowOverheadProducer getLOProducer(String topic, String compression)
	    throws Exception {
	Properties producerProps = new Properties();
	producerProps.setProperty("metadata.broker.list", "localhost:9876");
	producerProps.setProperty("compression.code", compression);
	producerProps.setProperty("queue.buffering.max.ms", "100");
	ProducerConfiguration producerConf = new ProducerConfiguration(
		producerProps);
	LowOverheadProducer producer = new LowOverheadProducer(producerConf,
		"myclient", topic, "mykey", null);
	return producer;
    }

    private ConsumerConnector getStdConsumer() {
	Properties props = new Properties();
	props.put("zookeeper.connect", "localhost:21818");
	props.put("group.id", "test");
	ConsumerConfig conf = new ConsumerConfig(props);
	return Consumer.createJavaConsumerConnector(conf);
    }

    private LowOverheadConsumer getLOConsumer(String topic, int partition)
	    throws Exception {
	Properties props = new Properties();
	props.setProperty("metadata.broker.list", "localhost:9876");
	ConsumerConfiguration conf = new ConsumerConfiguration(props);
	return new LowOverheadConsumer(conf, "test-client", topic, partition);
    }

    // Sanity check. Standard producer and consumer
    @Test
    public void testStdProducerStdConsumer() throws Throwable {
	for (String compression : COMPRESSION_METHODS) {
	    final String topic = "std-std-" + compression;
	    setupTopic(topic);

	    ConsumerConnector consumer = getStdConsumer();
	    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	    topicCountMap.put(topic, 1);
	    final Map<String, List<KafkaStream<byte[], byte[]>>> streams = consumer
		    .createMessageStreams(topicCountMap);

	    error = null;
	    Thread t = new Thread(new Runnable() {
		@Override
		public void run() {
		    try {
			ConsumerIterator<byte[], byte[]> it = streams
				.get(topic).get(0).iterator();

			for (int i = 0; i < logs.size(); i++) {
			    String line = new String(it.next().message());
			    String message = line.split(" ", 4)[3].trim();
			    assertEquals(logs.get(i), message);
			}
		    } catch (Throwable t) {
			setError(t);
		    }
		}
	    });
	    t.start();
	    Thread.sleep(100);

	    Producer<String, String> producer = getStdProducer(compression);
	    for (String log : logs) {
		producer.send(new KeyedMessage<String, String>(topic, "mykey",
			System.currentTimeMillis() + " test 123 " + log));
	    }

	    t.join();
	    if (error != null) {
		throw error;
	    }
	}
    }

    @Test
    public void testLOProducerStdConsumer() throws Throwable {
	for (String compression : COMPRESSION_METHODS) {
	    final String topic = "lop-std-" + compression;
	    setupTopic(topic);

	    ConsumerConnector consumer = getStdConsumer();
	    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	    topicCountMap.put(topic, 1);
	    final Map<String, List<KafkaStream<byte[], byte[]>>> streams = consumer
		    .createMessageStreams(topicCountMap);

	    error = null;
	    Thread t = new Thread(new Runnable() {
		@Override
		public void run() {
		    try {
			ConsumerIterator<byte[], byte[]> it = streams
				.get(topic).get(0).iterator();

			for (int i = 0; i < logs.size(); i++) {
			    String line = new String(it.next().message());
			    String message = line.split(" ", 4)[3].trim();
			    assertEquals(logs.get(i), message);
			}
		    } catch (Throwable t) {
			setError(t);
		    }
		}
	    });
	    t.start();
	    Thread.sleep(100);

	    LowOverheadProducer producer = getLOProducer(topic, compression);
	    for (String log : logs) {
		byte[] msg = (System.currentTimeMillis() + " test 123 " + log)
			.getBytes();
		producer.send(msg, 0, msg.length);
	    }

	    t.join();
	    if (error != null) {
		throw error;
	    }
	}
    }

    @Test
    public void testStdProducerLOConsumer() throws Throwable {
	for (String compression : COMPRESSION_METHODS) {
	    final String topic = "std-loc-" + compression;
	    setupTopic(topic);

	    final LowOverheadConsumer consumer = getLOConsumer(topic, 0);

	    error = null;
	    Thread t = new Thread(new Runnable() {
		@Override
		public void run() {
		    try {
			byte[] bytes = new byte[1024 * 1024];
			String line;
			String message;
			int messageLength;
			for (int i = 0; i < logs.size(); i++) {
			    messageLength = consumer.getMessage(bytes, 0,
				    bytes.length);
			    line = new String(bytes, 0, messageLength);
			    message = line.split(" ", 4)[3].trim();
			    assertEquals(logs.get(i), message);
			}
		    } catch (Throwable t) {
			setError(t);
		    }
		}
	    });
	    t.start();
	    // TODO: this sleep just begs for race conditions. We should be
	    // waiting
	    // for the consumer to confirm that it's up, not just waiting a bit
	    // of
	    // time.
	    Thread.sleep(100);

	    Producer<String, String> producer = getStdProducer(compression);
	    for (String log : logs) {
		producer.send(new KeyedMessage<String, String>(topic, "mykey",
			System.currentTimeMillis() + " test 123 " + log));
	    }

	    t.join();
	    if (error != null) {
		throw error;
	    }
	}
    }

    @Test
    public void testLOProducerLOConsumer() throws Throwable {
	for (String compression : COMPRESSION_METHODS) {
	    final String topic = "std-loc-" + compression;
	    setupTopic(topic);

	    final LowOverheadConsumer consumer = getLOConsumer(topic, 0);

	    error = null;
	    Thread t = new Thread(new Runnable() {
		@Override
		public void run() {
		    try {
			byte[] bytes = new byte[1024 * 1024];
			String line;
			String message;
			int messageLength;
			for (int i = 0; i < logs.size(); i++) {
			    messageLength = consumer.getMessage(bytes, 0,
				    bytes.length);
			    line = new String(bytes, 0, messageLength);
			    message = line.split(" ", 4)[3].trim();
			    assertEquals(logs.get(i), message);
			}
		    } catch (Throwable t) {
			setError(t);
		    }
		}
	    });
	    t.start();
	    // TODO: this sleep just begs for race conditions. We should be
	    // waiting
	    // for the consumer to confirm that it's up, not just waiting a bit
	    // of
	    // time.
	    Thread.sleep(100);

	    LowOverheadProducer producer = getLOProducer(topic, compression);
	    for (String log : logs) {
		byte[] msg = (System.currentTimeMillis() + " test 123 " + log)
			.getBytes();
		producer.send(msg, 0, msg.length);
	    }

	    t.join();
	    if (error != null) {
		throw error;
	    }
	}
    }

    @Test
    public void testAppender() throws Throwable {
	final String topic = "log4j-test";
	setupTopic(topic);

	ConsumerConnector consumer = getStdConsumer();
	Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	topicCountMap.put(topic, 1);
	final Map<String, List<KafkaStream<byte[], byte[]>>> streams = consumer
		.createMessageStreams(topicCountMap);

	error = null;
	Thread t = new Thread(new Runnable() {
	    @Override
	    public void run() {
		try {
		    ConsumerIterator<byte[], byte[]> it = streams.get(topic)
			    .get(0).iterator();

		    for (int i = 0; i < logs.size(); i++) {
			String line = new String(it.next().message());
			String message = line.split(" ", 4)[3].trim();
			assertEquals(logs.get(i), message);
		    }
		} catch (Throwable t) {
		    setError(t);
		}
	    }

	});
	t.start();
	Thread.sleep(100);

	Logger logger = Logger.getLogger("test.appender");
	for (String log : logs) {
	    logger.info(log);
	}

	t.join();
	if (error != null) {
	    throw error;
	}
    }

    private void setError(Throwable t) {
	error = t;
    }

}
