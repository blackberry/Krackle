package com.blackberry.log4j;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.spi.LoggingEvent;

import com.blackberry.kafka.lowoverhead.producer.LowOverheadProducer;
import com.blackberry.kafka.lowoverhead.producer.ProducerConfiguration;

public class KafkaAppender extends AppenderSkeleton {
    private static final Charset UTF8 = Charset.forName("UTF8");

    private Logger logger;

    private Properties props = new Properties();
    private String clientId = null;
    private String topic;
    private String key = null;

    private LowOverheadProducer producer;

    @Override
    public void activateOptions() {
	PropertyConfigurator.configure(this.getClass().getClassLoader()
		.getResource("kafka.appender.log4j.properties"));
	logger = Logger.getLogger(this.getClass());

	// clientid and key default to the hostname
	if (clientId == null) {
	    try {
		clientId = InetAddress.getLocalHost().getHostName();
		props.setProperty("client.id", clientId);
	    } catch (UnknownHostException e) {
		logger.error("Error getting hostname for default clientId while configuring "
			+ this.getClass());
	    }
	}
	if (key == null) {
	    try {
		key = InetAddress.getLocalHost().getHostName();
		props.setProperty("key", key);
	    } catch (UnknownHostException e) {
		logger.error("Error getting hostname for default key while configuring "
			+ this.getClass());
	    }
	}

	ProducerConfiguration conf = null;
	try {
	    conf = new ProducerConfiguration(props);
	} catch (Exception e) {
	    logger.error("Error creating " + LowOverheadProducer.class
		    + ".  Cannot log to Kafka.", e);
	}

	try {
	    producer = new LowOverheadProducer(conf, clientId, topic, key, null);
	} catch (Exception e) {
	    logger.error("Error creating " + LowOverheadProducer.class
		    + ".  Cannot log to Kafka.", e);
	}
    }

    @Override
    public void close() {
	producer.close();
    }

    @Override
    public boolean requiresLayout() {
	return true;
    }

    private byte[] message;

    @Override
    protected void append(LoggingEvent e) {
	message = getLayout().format(e).getBytes(UTF8);

	try {
	    producer.send(message, 0, message.length);
	} catch (Throwable t) {
	    logger.error("Error sending log to Kafka.", t);
	}
    }

    public void setClientId(String clientId) {
	this.clientId = clientId;
    }

    public void setTopic(String topic) {
	this.topic = topic;
    }

    public void setKey(String key) {
	this.key = key;
    }

    public void setMetadataBrokerList(String metadataBrokerList) {
	props.setProperty("metadata.broker.list", metadataBrokerList);
    }

    public void setQueueBufferingMaxMs(String queueBufferingMaxMs) {
	props.setProperty("queue.buffering.max.ms", queueBufferingMaxMs);
    }

    public void setRequrestRequiredAcks(String requestRequiredAcks) {
	props.setProperty("request.required.acks", requestRequiredAcks);
    }

    public void setRequestTimeoutMs(String requestTimeoutMs) {
	props.setProperty("request.timeout.ms", requestTimeoutMs);
    }

    public void setMessageSendMaxRetries(String messageSendMaxRetries) {
	props.setProperty("message.send.max.retries", messageSendMaxRetries);
    }

    public void setRetryBackoffMs(String retryBackoffMs) {
	props.setProperty("retry.backoff.ms", retryBackoffMs);
    }

    public void setMessageBufferSize(String messageBufferSize) {
	props.setProperty("message.buffer.size", messageBufferSize);
    }

    public void setSendBufferSize(String sendBufferSize) {
	props.setProperty("send.buffer.size", sendBufferSize);
    }

    public void setResponseBufferSize(String responseBufferSize) {
	props.setProperty("response.buffer.size", responseBufferSize);
    }

    public void setCompressionCodec(String compressionCodec) {
	props.setProperty("compression.codec", compressionCodec);
    }

    public void setCompressionLevel(String compressionLevel) {
	props.setProperty("compression.level", compressionLevel);
    }

    public void setTopicMetadataRefreshIntervalMs(
	    String topicMetadataRefreshIntervalMs) {
	props.setProperty("topic.metadata.refresh.interval.ms",
		topicMetadataRefreshIntervalMs);
    }

    public void setMetricsToConsole(String metricsToConsole) {
	props.setProperty("metrics.to.console", metricsToConsole);
    }

    public void setMetricsToConsoleIntervalMs(String metricsToConsoleIntervalMs) {
	props.setProperty("metrics.to.console.interval.ms",
		metricsToConsoleIntervalMs);
    }

}
