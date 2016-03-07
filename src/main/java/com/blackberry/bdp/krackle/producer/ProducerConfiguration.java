/**
 * Copyright 2014 BlackBerry, Limited.
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
package com.blackberry.bdp.krackle.producer;

import com.blackberry.bdp.krackle.auth.AuthenticatedSocketSingleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.zip.Deflater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for a producer.
 *
 * Many of these properties are the same as those in the standard Java client, as documented at http://kafka.apache.org/documentation.html#producerconfigs
 *
 * <p><b>NOTE:</b> Every single one of these properties can be overwritten for a specific topic by using the following property patten:</p>
 *
 * <p>source.&lt;<i>topic</i>&gt.&lt;<i>property</i>&gt</p>
 *
 * <p>Valid properties are</p>
 *
 * <table border="1">
 *
 * <tr>
 * <th>property</th>
 * <th>default</th>
 * <th>description</th>
 * </tr>
 *
 * <tr>
 * <td>metadata.broker.list</td>
 * <td></td>
 * <td>(required) A comma separated list of seed brokers to connect to in order to get metadata about the cluster.</td>
 * </tr>
 *
 * <tr>
 * <td>queue.buffering.max.ms</td>
 * <td>5000</td>
 * <td>Maximum time to buffer data. For example a setting of 100 will try to batch together 100ms of messages to send at once. This will improve throughput but adds message delivery latency due to the buffering.</td>
 * </tr>
 *
 * <tr>
 * <td>request.required.acks</td>
 * <td>1</td>
 * <td>This value controls when a produce request is considered completed. Specifically, how many other brokers must have committed the data to their log and acknowledged this to the leader? Typical values are
 * <ul>
 * <li>0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).</li>
 * <li>1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).</li>
 * <li>-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.</li>
 * </ul>
 * </td>
 * </tr>
 *
 * <tr>
 * <td>request.timeout.ms</td>
 * <td>10000</td>
 * <td>The amount of time the broker will wait trying to meet the request.required.acks requirement before sending back an error to the client.
 * </td>
 * </tr>
 *
 * <tr>
 * <td>message.send.max.retries</td>
 * <td>3</td>
 * <td>This property will cause the producer to automatically retry a failed send request. This property specifies the number of retries when such failures occur. Note that setting a non-zero value here can lead to duplicates in the case of network errors that cause a message to be sent but the acknowledgement to be lost.</td>
 * </tr>
 *
 * <tr>
 * <td>retry.backoff.ms</td>
 * <td>100</td>
 * <td>Before each retry, the producer refreshes the metadata of relevant topics to see if a new leader has been elected. Since leader election takes a bit of time, this property specifies the amount of time that the producer waits before refreshing the metadata.</td>
 * </tr>
 *
 * <tr>
 * <td>topic.metadata.refresh.interval.ms</td>
 * <td>60 * 10 * 1000</td>
 * <td>The producer generally refreshes the topic metadata from brokers when there is a failure (partition missing, leader not available...). It will also poll regularly (default: every 10min so 600000ms). If you set this to a negative value, metadata will only get refreshed on failure. If you set this to zero, the metadata will get refreshed after each message sent (not recommended). Important note: the refresh happen only AFTER the message is sent, so if the producer never sends a message the metadata is never refreshed</td>
 * </tr>
 *
 * <tr>
 * <td>message.buffer.size</td>
 * <td>1024*1024</td>
 * <td>The size of each buffer that is used to store raw messages before they are sent. Since a full buffer is sent at once, don't make this too big.</td>
 * </tr>
 *
 * <tr>
 * <td>use.shared.buffers</td>
 * <td>false</td>
 * <td>If this is set to true, then there will be one set of buffers that is used by all Producer instances. In that case, ensure that num.buffers is large enough to accommodate this.</td>
 * </tr>
 *
 * <tr>
 * <td>num.buffers</td>
 * <td>2</td>
 * <td>The number of buffers to use. At any given time, there is up to one buffer being filled with new data, up to one buffer having its data sent to the broker, and any number of buffers waiting to be filled and/or sent.
 *
 * Essentially, the limit of the amount of data that can be queued at at any given time is message.buffer.size num.buffers. Although, in reality, you won't get buffers to 100% full each time.
 *
 * If use.shared.buffers=false, then this many buffers will be allocated per Producer. If use.shared.buffers=true then this is the total for the JVM.</td>
 * </tr>
 *
 * <tr>
 * <td>send.buffer.size</td>
 * <td>1.5 * 1024 * 1024</td>
 * <td>Size of the byte buffer used to store the final (with headers and compression applied) data to be sent to the broker.</td>
 *
 * </tr>
 * <tr>
 * <td>sender.threads</td>
 * <td>1</td>
 * <td>How many threads to use for sending to the broker.  A larger number can be useful for higher latency high volume topics however message order cannot be guaranteed</td>
 * </tr>
 *
 * <tr>
 * <td>compression.codec</td>
 * <td>none</td>
 * <td>This parameter allows you to specify the compression codec for all data generated by this producer. Valid values are "none", "gzip" and "snappy".</td>
 * </tr>
 *
 * <tr>
 * <td>gzip.compression.level</td>
 * <td>java.util.zip.Deflater.DEFAULT_COMPRESSION</td>
 * <td>If compression.codec is set to gzip, then this allows configuration of the compression level.
 * <ul>
 * <li>-1: default compression level</li>
 * <li>0: no compression</li>
 * <li>1-9: 1=fastest compression ... 9=best compression</li>
 * </ul>
 * </td>
 * </tr>
 *
 * <tr>
 * <td>queue.enqueue.timeout.ms</td>
 * <td>-1</td>
 * <td>The amount of time to block before dropping messages when all buffers are full. If set to 0 events will be enqueued immediately or dropped if the queue is full (the producer send call will never block). If set to -1 the producer will block indefinitely and never willingly drop a send.</td>
 * </tr>
 *
 * </table>
 *
 * <p><b>NOTE: Quick rotate, rotate, and partition rotation in General</b></p>

 * <p>Quick rotate is no longer a supported configuration item.  Instead all topic meta data refreshes will rotate partitions and if quicker rotation is required than topic.metadata.refresh.interval.ms can be configured accordingly.  Regular topic specific overrides are possible as well for topics that require faster rotaiton.</p>

 *
 */
public class ProducerConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(ProducerConfiguration.class);

	protected static final int ONE_MB = 1024 * 1024;
	private final Properties props;
	public String topicName;

	// Options matching the producer client
	private List<String> metadataBrokerList;
	private short requestRequiredAcks;
	private int requestTimeoutMs;
	private int initialSocketConnectionTimeoutMs;

	private String compressionCodec;
	private int messageSendMaxRetries;
	private int retryBackoffMs;
	private final int retryBackoffExponent;

	private int senderThreads;
	private int partitionsRotate;
	private long topicMetadataRefreshIntervalMs;
	private long queueBufferingMaxMs;
	private long queueEnqueueTimeoutMs;

	// Client specific options
	private int messageBufferSize;
	private int numBuffers;
	private int sendBufferSize;
	private int compressionLevel;
	private boolean useSharedBuffers;

	/**
	 * ProducerConfiguration class that supports parsing properties that all support
	 * being prefixed with a topic name for overriding default values per topic as required
	 * @param props the properties object  to parse
	 * @param topicName the topic being configured
	 * @throws Exception
	 */
	public ProducerConfiguration(Properties props,
		 String topicName) throws Exception {
		this.props = props;
		this.topicName = topicName;

		LOG.info("Building configuration.");

		metadataBrokerList = parseMetadataBrokerList("metadata.broker.list");
		queueBufferingMaxMs = parseQueueBufferingMaxMs("queue.buffering.max.ms", "5000");
		requestRequiredAcks = parseRequestRequiredAcks("request.required.acks", "1");
		requestTimeoutMs = parseRequestTimeoutMs("request.timeout.ms", "10000");
		initialSocketConnectionTimeoutMs = Integer.parseInt(props.getProperty("initial.socket.connection.timeout.ms", "3000"));
		messageSendMaxRetries = parseMessageSendMaxRetries("message.send.max.retries", "3");
		retryBackoffMs = parseRetryBackoffMs("retry.backoff.ms", "100");
		retryBackoffExponent = parseRetryBackoffMs("retry.backoff.exponent", "1");
		topicMetadataRefreshIntervalMs = parseTopicMetadataRefreshIntervalMs("topic.metadata.refresh.interval.ms", "" + (60 * 10 * 1000));
		partitionsRotate = parsePartitionsRotate("partitions.rotate", "random");
		sendBufferSize = parseSendBufferSize("send.buffer.size", "" + (int) (1.5 * 1024 * 1024));
		compressionCodec = parseCompressionCodec("compression.codec", "none");
		compressionLevel = parsecCmpressionLevel("gzip.compression.level", "" + Deflater.DEFAULT_COMPRESSION);
		queueEnqueueTimeoutMs = parseQueueEnqueueTimeoutMs("queue.enqueue.timeout.ms", "-1");
		senderThreads = parseSenderThreads("sender.threads", "1");

		// The (receive) buffers are a special story, so we'll parse and set them in one go.
		parseAndSetBuffers("use.shared.buffers", "false", "message.buffer.size", "" + ONE_MB, "num.buffers", "2");

		AuthenticatedSocketSingleton.getInstance().configure(props);
	}


	/**
	 *
	 * @param propName the property name to obtain a topic aware override for
	 * @return The name of the property
	 */
	public String getTopicAwarePropName(String propName) {
		if (getTopicName() == null) {
			return propName;
		}

		String topicPropName = String.format("source.%s.%s", getTopicName(), propName);

		if (props.containsKey(topicPropName)) {
			LOG.debug("topic specific property {} exists that overrides {}  ", topicPropName, propName);
			return topicPropName;
		}

		return propName;
	}

	private List<String> parseMetadataBrokerList(String propName) throws Exception {
		List<String> myMetadataBrokerList = new ArrayList<>();
		String propNameBrokerList = getTopicAwarePropName(propName);
		String metadataBrokerListString = props.getProperty(propNameBrokerList);

		if (metadataBrokerListString == null || metadataBrokerListString.isEmpty()) {
			throw new Exception(String.format("%s cannot be empty", propNameBrokerList));
		}

		for (String s : metadataBrokerListString.split(",")) {
			if (s.matches("^[\\.a-zA-Z0-9-]*:\\d+$")) {
				myMetadataBrokerList.add(s);
			} else {
				throw new Exception(String.format(
					 "%s must contain a comma separated list of host:port, without spaces).  Got %s",
					 propNameBrokerList, metadataBrokerListString));
			}
		}

		LOG.info("{} = {}", propNameBrokerList, myMetadataBrokerList);

		return myMetadataBrokerList;
	}

	private Long parseQueueBufferingMaxMs(String propName, String defaultValue) throws Exception {
		String propNameQueueBufferignMaxMs = getTopicAwarePropName(propName);
		Long myQueueBufferingMaxMs = Long.parseLong(props.getProperty(propNameQueueBufferignMaxMs, defaultValue));

		if (myQueueBufferingMaxMs < 0) {
			throw new Exception(String.format("%s cannot be negative", propNameQueueBufferignMaxMs));
		}

		LOG.info("{}  = {}", propNameQueueBufferignMaxMs, myQueueBufferingMaxMs);
		return myQueueBufferingMaxMs;
	}

	private Short parseRequestRequiredAcks(String propName, String defaultValue) throws Exception {
		String acksPropertyName = getTopicAwarePropName(propName);
		Short myRequestRequiredAcks = Short.parseShort(props.getProperty(acksPropertyName, defaultValue));

		if (myRequestRequiredAcks != -1 && myRequestRequiredAcks != 0 && myRequestRequiredAcks != 1) {
			throw new Exception(String.format("%s can only be -1, 0 or 1.  Got %s", acksPropertyName, myRequestRequiredAcks));
		}

		LOG.info("{} = {}", acksPropertyName, myRequestRequiredAcks);
		return myRequestRequiredAcks;
	}

	private Integer parseRequestTimeoutMs(String propName, String defaultValue) throws Exception {
		String propNameRequestTimeoutMs = getTopicAwarePropName(propName);
		Integer myRequestTimeoutMs = Integer.parseInt(props.getProperty(propNameRequestTimeoutMs, defaultValue));

		if (myRequestTimeoutMs < 0) {
			throw new Exception(String.format("%s cannot  be negative.  Got %s ", propNameRequestTimeoutMs, myRequestTimeoutMs));
		}

		LOG.info("{} = {}", propNameRequestTimeoutMs, myRequestTimeoutMs);
		return myRequestTimeoutMs;
	}

	private Integer parseMessageSendMaxRetries(String propName, String defaultValue) throws Exception {
		String propNameSendMaxRetries = getTopicAwarePropName(propName);
		Integer myMessageSendMaxRetries = Integer.parseInt(props.getProperty(propNameSendMaxRetries, defaultValue));

		if (myMessageSendMaxRetries < 0) {
			throw new Exception(String.format("%s cannot be negative.  Got %s", propNameSendMaxRetries, myMessageSendMaxRetries));
		}

		LOG.info("{} = {}", propNameSendMaxRetries, myMessageSendMaxRetries);
		return myMessageSendMaxRetries;
	}

	private Integer parseRetryBackoffMs(String propName, String defaultValue) throws Exception {
		String propNameRetryBackoffMs = getTopicAwarePropName(propName);
		Integer myRetryBackoffMs = Integer.parseInt(props.getProperty(propNameRetryBackoffMs, defaultValue));

		if (myRetryBackoffMs < 0) {
			throw new Exception(String.format("%s  cannot be negative.  Got  %s", propNameRetryBackoffMs, myRetryBackoffMs));
		}

		LOG.info("{} = {}", propNameRetryBackoffMs, myRetryBackoffMs);
		return myRetryBackoffMs;
	}

	private Long parseTopicMetadataRefreshIntervalMs(String propName, String defaultValue) throws Exception {
		String propNameTopicMetadataRefreshIntervalMs = getTopicAwarePropName(propName);
		Long myTopicMetadataRefreshIntervalMs = Long.parseLong(props.getProperty(propNameTopicMetadataRefreshIntervalMs, defaultValue));
		LOG.info("{} = {}", propNameTopicMetadataRefreshIntervalMs, myTopicMetadataRefreshIntervalMs);
		return myTopicMetadataRefreshIntervalMs;
	}

	private int parsePartitionsRotate(String propName, String defaultValue) throws Exception {
		String propNamePartitionsRotate = getTopicAwarePropName(propName);

		String myPartitionsRotateString = props.getProperty(propNamePartitionsRotate, defaultValue);
		int myPartitionsRotate = 0;

		if (myPartitionsRotateString.compareToIgnoreCase("false") == 0) {
			myPartitionsRotate = 0;
		} else {
			if (myPartitionsRotateString.compareToIgnoreCase("true") == 0) {
				myPartitionsRotate = 1;
			} else {
				if (myPartitionsRotateString.compareToIgnoreCase("random") == 0) {
					myPartitionsRotate = 2;
				} else {
					throw new Exception(String.format("%s must be one of false, true, random.  Got %s", propNamePartitionsRotate, myPartitionsRotateString));
				}
			}
		}

		LOG.info("{} = {}", propNamePartitionsRotate, myPartitionsRotateString);
		return myPartitionsRotate;
	}

	private int parseSenderThreads(String propName, String defaultValue) throws Exception {
		String propNameTopicSenderThreads = getTopicAwarePropName(propName);
		Integer myTopicSenderThreads = Integer.parseInt(props.getProperty(propNameTopicSenderThreads, defaultValue));
		LOG.info("{} = {}", propNameTopicSenderThreads, myTopicSenderThreads);
		return myTopicSenderThreads;
	}

	private void parseAndSetBuffers(
		 String sharedBuffersPropName,
		 String sharedBuffersDefault,
		 String defaultPropNameBufferSize,
		 String defaultBufferSize,
		 String defaultPropNameNumBuffers,
		 String defaultNumBuffers) throws Exception {
		/**
		 *
		 * The Buffers Story:
		 *
		 * We may be using shared buffers HOWEVER a topic can specify it's own num.buffers
		 * and message.buffer.size and it's buffers become ently private.  If that's the case
		 * then we need to force useSharedBuffers=false and the remaining topic aware
		 * property naming does the rest.  In theory, it's brilliant.  In reality we'll see...
		 *
		 */

		useSharedBuffers = Boolean.parseBoolean(props.getProperty(sharedBuffersPropName, sharedBuffersDefault).trim());

		LOG.info("The global/non-topic aware property {}  = {}", sharedBuffersPropName, useSharedBuffers);

		String propNameBufferSize = getTopicAwarePropName(defaultPropNameBufferSize);
		String propNameNumBuffers = getTopicAwarePropName(defaultPropNameNumBuffers);

		/**
		 * Ensure both number of buffers and buffer size are either default property names
		 * or have topic specific properties defined.  You can't overwrite one as topic specific
		 * without overwriting the other.
		 */
		if (propNameBufferSize.equals(defaultPropNameBufferSize) ^ propNameNumBuffers.equals(defaultPropNameNumBuffers)) {
			throw new Exception(String.format("%s and %s specified, cannot mix topic specific and global properties",
				 propNameBufferSize, propNameNumBuffers));
		}

		if (false == (propNameNumBuffers.equals(defaultPropNameNumBuffers) && propNameBufferSize.equals(defaultPropNameBufferSize))) {
			useSharedBuffers = false;
			LOG.warn("{} = {}, and {} = {}", propNameBufferSize, defaultPropNameBufferSize, propNameNumBuffers, defaultPropNameNumBuffers);
			LOG.warn("{} forcing inherently private buffers as topic specific configuration exists", getTopicName());
		}

		messageBufferSize = Integer.parseInt(props.getProperty(propNameBufferSize, defaultBufferSize));

		if (messageBufferSize < 1) {
			throw new Exception(String.format("%s must be greater than 0.  Got %s", propNameBufferSize, messageBufferSize));
		}

		LOG.info("{} = {}", propNameBufferSize, messageBufferSize);

		numBuffers = Integer.parseInt(props.getProperty(propNameNumBuffers, defaultNumBuffers));

		if (numBuffers < 2) {
			throw new Exception(String.format("%s must be at least 2.  Got %s", propNameNumBuffers, numBuffers));
		}

		LOG.info("{} = {}", propNameNumBuffers, numBuffers);
	}

	private Integer parseSendBufferSize(String propName, String defaultValue) throws Exception {
		String propNameSendBufferSize = getTopicAwarePropName(propName);
		Integer mySendBufferSize = Integer.parseInt(props.getProperty(propNameSendBufferSize, defaultValue));

		if (mySendBufferSize < 1) {
			throw new Exception(String.format("%s must be greater than 0.  Got %s", propNameSendBufferSize, mySendBufferSize));
		}

		LOG.info("{} = {}", propNameSendBufferSize, mySendBufferSize);
		return mySendBufferSize;
	}

	private String parseCompressionCodec(String propName, String defaultValue) throws Exception {
		String propNameRawCompressionCodec = getTopicAwarePropName(propName);
		String rawCompressionCodec = props.getProperty(propNameRawCompressionCodec, defaultValue);
		String myCompressionCodec = rawCompressionCodec.toLowerCase();

		if (myCompressionCodec.equals("none") == false
			 && myCompressionCodec.equals("gzip") == false
			 && myCompressionCodec.equals("snappy") == false) {
			throw new Exception(String.format("%s must be one of none, gzip or snappy.  Got  %s", propNameRawCompressionCodec, myCompressionCodec));
		}

		LOG.info("{} = {}", propNameRawCompressionCodec, myCompressionCodec);
		return myCompressionCodec;
	}

	private Integer parsecCmpressionLevel(String propName, String defaultValue) throws Exception {
		String propNameCompressionLevel = getTopicAwarePropName(propName);
		Integer myCompressionLevel = Integer.parseInt(props.getProperty(propNameCompressionLevel, defaultValue));

		if (myCompressionLevel < -1 || myCompressionLevel > 9) {
			throw new Exception(String.format("%s must be -1 (default), 0 (no compression) or in the range 1-9.  Got %s", propNameCompressionLevel, myCompressionLevel));
		}

		LOG.info("{} = {}", propNameCompressionLevel, myCompressionLevel);
		return myCompressionLevel;
	}

	private Long parseQueueEnqueueTimeoutMs(String propName, String defaultValue) throws Exception {
		String propNameQueueEnqueueTimeoutMs = getTopicAwarePropName(propName);
		Long myQueueEnqueueTimeoutMs = Long.parseLong(props.getProperty(propNameQueueEnqueueTimeoutMs, defaultValue));

		if (myQueueEnqueueTimeoutMs != -1 && myQueueEnqueueTimeoutMs < 0) {
			throw new Exception(String.format("%s must either be -1 or a non-negative.  Got %s", propNameQueueEnqueueTimeoutMs, myQueueEnqueueTimeoutMs));
		}

		LOG.info("{} = {}", propNameQueueEnqueueTimeoutMs, myQueueEnqueueTimeoutMs);
		return myQueueEnqueueTimeoutMs;
	}

	/**
	 *
	 * @return the metadata broker list
	 */
	public List<String> getMetadataBrokerList() {
		return metadataBrokerList;
	}

	/**
	 *
	 * @param metadataBrokerList the metadata broker list
	 */
	public void setMetadataBrokerList(List<String> metadataBrokerList) {
		this.metadataBrokerList = metadataBrokerList;
	}

	/**
	 *
	 * @return the queue  buffering max milliseconds
	 */
	public long getQueueBufferingMaxMs() {
		return queueBufferingMaxMs;
	}

	/**
	 *
	 * @param queueBufferingMaxMs the queue  buffering max milliseconds
	 */
	public void setQueueBufferingMaxMs(long queueBufferingMaxMs) {
		this.queueBufferingMaxMs = queueBufferingMaxMs;
	}

	/**
	 *
	 * @return the request required acks
	 */
	public short getRequestRequiredAcks() {
		return requestRequiredAcks;
	}

	/**
	 *
	 * @param requestRequiredAcks the request required acks
	 */
	public void setRequestRequiredAcks(short requestRequiredAcks) {
		this.requestRequiredAcks = requestRequiredAcks;
	}

	/**
	 *
	 * @return the request timeout milliseconds
	 */
	public int getRequestTimeoutMs() {
		return requestTimeoutMs;
	}

	/**
	 *
	 * @param requestTimeoutMs the request timeout milliseconds
	 */
	public void setRequestTimeoutMs(int requestTimeoutMs) {
		this.requestTimeoutMs = requestTimeoutMs;
	}

	/**
	 *
	 * @return message send max retries
	 */
	public int getMessageSendMaxRetries() {
		return messageSendMaxRetries;
	}

	/**
	 *
	 * @param messageSendMaxRetries message send max retries
	 */
	public void setMessageSendMaxRetries(int messageSendMaxRetries) {
		this.messageSendMaxRetries = messageSendMaxRetries;
	}

	/**
	 *
	 * @return the number of threads sending to the broker
	 */
	public int getSenderThreads() {
		return senderThreads;
	}

	/**
	 *
	 * @param senderThreads the number of threads sending to the broker
	 */
	public void setSenderThreads(int senderThreads) {
		this.senderThreads = senderThreads;
	}

	/**
	 *
	 * @return milliseconds to back off for when retrying
	 */
	public int getRetryBackoffMs() {
		return retryBackoffMs;
	}

	/**
	 *
	 * @param retryBackoffMs milliseconds to back off for when retrying
	 */
	public void setRetryBackoffMs(int retryBackoffMs) {
		this.retryBackoffMs = retryBackoffMs;
	}

	/**
	 *
	 * @return message buffer size
	 */
	public int getMessageBufferSize() {
		return messageBufferSize;
	}

	/**
	 *
	 * @param messageBufferSize message buffer size
	 */
	public void setMessageBufferSize(int messageBufferSize) {
		this.messageBufferSize = messageBufferSize;
	}

	/**
	 *
	 * @return are shared buffers being used?
	 */
	public boolean isUseSharedBuffers() {
		return useSharedBuffers;
	}

	/**
	 *
	 * @param useSharedBuffers the shared buffer use
	 */
	public void setUseSharedBuffers(boolean useSharedBuffers) {
		this.useSharedBuffers = useSharedBuffers;
	}

	/**
	 *
	 * @return the number of buffers
	 */
	public int getNumBuffers() {
		return numBuffers;
	}

	/**
	 *
	 * @param numBuffers the number of buffers
	 */
	public void setNumBuffers(int numBuffers) {
		this.numBuffers = numBuffers;
	}

	/**
	 *
	 * @return the send buffer size
	 */
	public int getSendBufferSize() {
		return sendBufferSize;
	}

	/**
	 *
	 * @param sendBufferSize the send buffer size
	 */
	public void setSendBufferSize(int sendBufferSize) {
		this.sendBufferSize = sendBufferSize;
	}

	/**
	 *
	 * @return the compression codec used
	 */
	public String getCompressionCodec() {
		return compressionCodec;
	}

	/**
	 *
	 * @param compressionCodec the compression codec used
	 */
	public void setCompressionCodec(String compressionCodec) {
		this.compressionCodec = compressionCodec;
	}

	/**
	 *
	 * @return the compression level
	 */
	public int getCompressionLevel() {
		return compressionLevel;
	}

	/**
	 *
	 * @param compressionLevel the compression level
	 */
	public void setCompressionLevel(int compressionLevel) {
		this.compressionLevel = compressionLevel;
	}

	/**
	 *
	 * @return the topic metadata refresh interval in milliseconds
	 */
	public long getTopicMetadataRefreshIntervalMs() {
		return topicMetadataRefreshIntervalMs;
	}

	/**
	 *
	 * @param topicMetadataRefreshIntervalMs the topic metadata refresh interval in milliseconds
	 */
	public void setTopicMetadataRefreshIntervalMs(long topicMetadataRefreshIntervalMs) {
		this.topicMetadataRefreshIntervalMs = topicMetadataRefreshIntervalMs;
	}

	/**
	 *
	 * @return the type of partitions rotation to use
	 */
	public int getPartitionsRotate() {
		return partitionsRotate;
	}

	/**
	 *
	 * @param partitionsRotate
	 */
	public void setPartitionsRotate(int partitionsRotate) {
		this.partitionsRotate = partitionsRotate;
	}

	/**
	 *
	 * @return the queue enqueue timeout milliseconds
	 */
	public long getQueueEnqueueTimeoutMs() {
		return queueEnqueueTimeoutMs;
	}

	/**
	 *
	 * @param queueEnqueueTimeoutMs the queue enqueue timeout milliseconds
	 */
	public void setQueueEnqueueTimeoutMs(long queueEnqueueTimeoutMs) {
		this.queueEnqueueTimeoutMs = queueEnqueueTimeoutMs;
	}

	/**
	 * @return the topicName
	 */
	public String getTopicName() {
		return topicName;
	}

	/**
	 * @param topicName the topicName to set
	 */
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	/**
	 * @return the retryBackoffExponent
	 */
	public int getRetryBackoffExponent() {
		return retryBackoffExponent;
	}

	/**
	 * @return the initialSocketConnectionTimeoutMs
	 */
	public int getInitialSocketConnectionTimeoutMs() {
		return initialSocketConnectionTimeoutMs;
	}

}
