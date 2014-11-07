/**
 * Copyright 2014 BlackBerry, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.blackberry.krackle.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.krackle.Constants;
import com.blackberry.krackle.KafkaError;
import com.blackberry.krackle.MetricRegistrySingleton;
import com.blackberry.krackle.meta.Broker;
import com.blackberry.krackle.meta.MetaData;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.net.SocketTimeoutException;

/**
 * An implementation of the Kafka 0.8 consumer.
 *
 * This class acts as a consumer of data from a cluster of Kafka brokers. Each instance only reads data from a single partition of a single topic. If you need to read more than that, then instantiate more instances.
 *
 * This class was designed to be very light weight. The standard Java client creates a lot of objects, and therefore causes a lot of garbage collection that leads to a major slowdown in performance. This client creates no new objects during steady state running, and so avoids all garbage collection overhead.
 */
public class Consumer
{
	private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
	private static final Charset UTF8 = Charset.forName("UTF8");

	private ConsumerConfiguration conf;

	private String clientId;
	private byte[] clientIdBytes;
	private short clientIdLength;

	private String topic;
	private byte[] topicBytes;
	private short topicLength;
	private Broker broker;
	private int partition;

	private MessageSetReader messageSetReader = new MessageSetReader();

	private long offset;
	private long lastOffset;
	private long highWaterMark;

	private byte[] offsetRequestBytes;
	private ByteBuffer offsetRequestBuffer;

	private byte[] offsetResponseBytes;
	private ByteBuffer offsetResponseBuffer;

	private byte[] requestBytes;
	private ByteBuffer requestBuffer;

	private int fetchMessageMaxBytes;
	private byte[] responseBytes;
	private ByteBuffer responseBuffer;

	private int fetchWaitMaxMs;
	private int fetchMinBytes;

	private MetricRegistry metrics;
	private Meter mMessageRequests = null;
	private Meter mMessageRequestsTotal = null;
	private Meter mMessagesReturned = null;
	private Meter mMessagesReturnedTotal = null;
	private Meter mBytesReturned = null;
	private Meter mBytesReturnedTotal = null;
	private Meter mMessageRequestsNoData = null;
	private Meter mMessageRequestsNoDataTotal = null;
	private Meter mBrokerReadAttempts = null;
	private Meter mBrokerReadAttemptsTotal = null;
	private Meter mBrokerReadSuccess = null;
	private Meter mBrokerReadSuccessTotal = null;
	private Meter mBrokerReadFailure = null;
	private Meter mBrokerReadFailureTotal = null;

	private int bytesReturned = 0;
	private Socket brokerSocket = null;
	private InputStream brokerIn = null;
	private OutputStream brokerOut = null;
	private int correlationId = 0;
	private int bytesRead;
	private int responseLength;
	private int responseCorrelationId;
	private short errorCode;
	private int messageSetSize;

	/*
	 * Create a new consumer that reads from a given consumer. It attempts to start at offset 0.
	 *
	 * @param conf ConsumerConfiguration for this consumer.
	 * @param clientId clientId to be send with requests to Kafka.
	 * @param topic topic to read from.
	 * @param partition id of the partition to read from.
	 */
	
	public Consumer(ConsumerConfiguration conf, String clientId, String topic, int partition)
	{
		this(conf, clientId, topic, partition, 0L);
	}

	/*
	 * Create a new consumer that reads from a given consumer that attempts to start reading at the given offset.
	 *
	 * @param conf ConsumerConfiguration for this consumer.
	 * @param clientId clientId to be send with requests to Kafka.
	 * @param topic topic to read from.
	 * @param partition id of the partition to read from.
	 * @param offset the offset to start reading from.
	 */
	
	public Consumer(ConsumerConfiguration conf, String clientId, String topic, int partition, long offset)
	{
		this(conf, clientId, topic, partition, offset, null);
	}

	/*
	 * Create a new consumer that reads from a given consumer that attempts to start reading at the given offset.
	 *
	 * Metrics are reported using the given instance of MetricRegistry instead the internal singleton instance.
	 *
	 * @param conf ConsumerConfiguration for this consumer.
	 * @param clientId clientId to be send with requests to Kafka.
	 * @param topic topic to read from.
	 * @param partition id of the partition to read from.
	 * @param offset the offset to start reading from.
	 * @param metrics the instance of MetricRegistry to use for reporting metrics.
	 */
	
	public Consumer(ConsumerConfiguration conf, String clientId, String topic, int partition, long offset, MetricRegistry metrics)
	{
		LOG.info("[{}-{}] creating consumer for  from offset {}", topic, partition, offset);

		this.conf = conf;

		if (metrics == null)
		{
			this.metrics = MetricRegistrySingleton.getInstance().getMetricsRegistry();
			MetricRegistrySingleton.getInstance().enableJmx();
			MetricRegistrySingleton.getInstance().enableConsole();
		} 
		else
		{
			this.metrics = metrics;
		}

		this.clientId = clientId;
		clientIdBytes = clientId.getBytes(UTF8);
		clientIdLength = (short) clientIdBytes.length;

		this.topic = topic;
		topicBytes = topic.getBytes(UTF8);
		topicLength = (short) topicBytes.length;

		this.partition = partition;
		this.offset = offset;

		initializeMetrics();

		offsetRequestBytes = new byte[44 + clientIdLength + topicLength];
		offsetRequestBuffer = ByteBuffer.wrap(offsetRequestBytes);

		offsetResponseBytes = new byte[32 + topicLength];
		offsetResponseBuffer = ByteBuffer.wrap(offsetResponseBytes);

		requestBytes = new byte[52 + clientIdLength + topicLength];
		requestBuffer = ByteBuffer.wrap(requestBytes);

		fetchMessageMaxBytes = conf.getFetchMessageMaxBytes();
		responseBytes = new byte[fetchMessageMaxBytes + 32 + topicLength];
		responseBuffer = ByteBuffer.wrap(responseBytes);

		fetchWaitMaxMs = conf.getFetchWaitMaxMs();
		fetchMinBytes = conf.getFetchMinBytes();

		LOG.info("[{}-{}] connecting to broker", topic, partition);
		connectToBroker();
	}

	private void initializeMetrics()
	{
		String name = topic + "-" + partition;

		mMessageRequests = this.metrics.meter("krackle:consumer:partitions:" + name + ":message requests");
		mMessageRequestsTotal = this.metrics.meter("krackle:consumer:total:message requests");
		mMessagesReturned = this.metrics.meter("krackle:consumer:partitions:" + name + ":message returned");
		mMessagesReturnedTotal = this.metrics.meter("krackle:consumer:total:message returned");
		mBytesReturned = this.metrics.meter("krackle:consumer:partitions:" + name + ":bytes returned");
		mBytesReturnedTotal = this.metrics.meter("krackle:consumer:total:bytes returned");
		mMessageRequestsNoData = this.metrics.meter("krackle:consumer:partitions:" + name + ":no message returned");
		mMessageRequestsNoDataTotal = this.metrics.meter("krackle:consumer:total:no message returned");
		mBrokerReadAttempts = this.metrics.meter("krackle:consumer:partitions:" + name + ":broker consume attempts");
		mBrokerReadAttemptsTotal = this.metrics.meter("krackle:consumer:total:broker consume attempts");
		mBrokerReadSuccess = this.metrics.meter("krackle:consumer:partitions:" + name + ":broker consume success");
		mBrokerReadSuccessTotal = this.metrics.meter("krackle:consumer:total:broker consume success");
		mBrokerReadFailure = this.metrics.meter("krackle:consumer:partitions:" + name + ":broker consume failure");
		mBrokerReadFailureTotal = this.metrics.meter("krackle:consumer:total:broker consume failure");
	}

	/*
	 * Read in a message from Kafka into the given byte array.
	 *
	 * If the size of the message exceeds maxLength, it will be truncated to fit.
	 *
	 * @param buffer the byte array to write into.
	 * @param pos the position in the byte array to write to.
	 * @param maxLength the max size of the message to write.
	 * @return the number of bytes writen, or <code>-1</code> if no data was returned.
	 * @throws IOException
	 */
	
	public int getMessage(byte[] buffer, int pos, int maxLength) throws IOException
	{
		mMessageRequests.mark();
		mMessageRequestsTotal.mark();

		try
		{
			if (messageSetReader == null || messageSetReader.isReady() == false)
			{
				readFromBroker();

				if (messageSetReader == null || messageSetReader.isReady() == false)
				{
					mMessageRequestsNoData.mark();
					mMessageRequestsNoDataTotal.mark();
					return -1;
				}
			}

			bytesReturned = messageSetReader.getMessage(buffer, pos, maxLength);

			if (bytesReturned == -1)
			{
				mMessageRequestsNoData.mark();
				mMessageRequestsNoDataTotal.mark();
				return -1;
			}

			lastOffset = messageSetReader.getOffset();
			offset = messageSetReader.getNextOffset();

			//LOG.info("message received from messageSetReader latOffset {} offset {}" , lastOffset, offset);

			mMessagesReturned.mark();
			mMessagesReturnedTotal.mark();
			mBytesReturned.mark(bytesReturned);
			mBytesReturnedTotal.mark(bytesReturned);

			return bytesReturned;
		}
		catch (SocketTimeoutException e)
		{
			LOG.error("[{}-{}] socket timeout to {}: {}", topic, partition, broker.getNiceDescription(), e);
			connectToBroker();
			return -1;
		}		
	}
	
	private void readFromBroker() throws IOException
	{
		mBrokerReadAttempts.mark();
		mBrokerReadAttemptsTotal.mark();

		if (brokerSocket == null || brokerSocket.isClosed())
		{
			LOG.info("[{}-{}] Connecting to broker", topic, partition);
			connectToBroker();
		}
		
		try
		{
			correlationId++;

			sendConsumeRequest(correlationId);
			receiveConsumeResponse(correlationId);

			mBrokerReadSuccess.mark();
			mBrokerReadSuccessTotal.mark();
		}
		catch (SocketTimeoutException e)
		{
			LOG.error("[{}-{}] socket timeout to {}", topic, partition, broker.getNiceDescription());
			connectToBroker();
		}
		catch (OffsetOutOfRangeException e)
		{
			mBrokerReadFailure.mark();
			mBrokerReadFailureTotal.mark();

			if (conf.getAutoOffsetReset().equals("smallest"))
			{
				LOG.warn("[{}-{}] offset {} out of range.  Resetting to the earliest offset available {}", topic, partition, offset, getEarliestOffset());
				offset = getEarliestOffset();
			} 
			else
			{
				if (conf.getAutoOffsetReset().equals("largest"))
				{
					LOG.warn("[{}-{}] Offset {} out of range.  Resetting to the latest offset available {}", topic, partition, offset, getLatestOffset());
					offset = getLatestOffset();
				} 
				else
				{
					LOG.error("[{}-{}] offset out of range and  not configured to auto reset", topic, partition);
					throw e;
				}
			}
		}
		catch (Exception e)
		{
			LOG.error("[{}-{}] error getting data from broker: ", topic, partition, e);

			if (brokerSocket != null)
			{
				try
				{
					brokerSocket.close();
				} 
				catch (IOException e1)
				{
					LOG.error("[{}-{}] error closing socket: ", topic, partition, e1);
				}
			}
			
			brokerSocket = null;
			mBrokerReadFailure.mark();
			mBrokerReadFailureTotal.mark();
		}
	}

	public long getEarliestOffset()
	{
		try
		{
			correlationId++;
			sendOffsetRequest(Constants.EARLIEST_OFFSET, correlationId);
			return getOffsetResponse(correlationId);
		}
		catch (SocketTimeoutException e)
		{
			LOG.error("[{}-{}] socket timeout to {}: {}", topic, partition, broker.getNiceDescription(), e);
			connectToBroker();
		}
		catch (IOException e)
		{
			LOG.error("[{}-{}] error getting earliest offset: ", topic, partition);
		}
		return 0L;
	}

	public long getLatestOffset()
	{
		try
		{
			correlationId++;
			sendOffsetRequest(Constants.LATEST_OFFSET, correlationId);
			return getOffsetResponse(correlationId);
		}
		catch (SocketTimeoutException e)
		{
			LOG.error("[{}-{}] socket timeout to {}: {}", topic, partition, broker.getNiceDescription(), e);
			connectToBroker();
		}
		catch (IOException e)
		{
			LOG.error("[{}-{}] error getting latest offset: ", topic, partition);
		}		
		return Long.MAX_VALUE;
	}

	private void sendOffsetRequest(long time, int correlationId) throws IOException
	{
		LOG.debug("Sending request for offset. correlation id = {}, time = {}", correlationId, time);

		offsetRequestBuffer.clear();

		// skip 4 bytes for length
		offsetRequestBuffer.position(4);

		// API key
		offsetRequestBuffer.putShort(Constants.APIKEY_OFFSET_REQUEST);

		// API Version
		offsetRequestBuffer.putShort(Constants.API_VERSION);

		// Correlation Id
		offsetRequestBuffer.putInt(correlationId);

		// ClientId
		offsetRequestBuffer.putShort(clientIdLength);
		offsetRequestBuffer.put(clientIdBytes);

		// replica id is always -1
		offsetRequestBuffer.putInt(-1);

		// Only requesting for 1 topic
		offsetRequestBuffer.putInt(1);

		// Topic Name
		offsetRequestBuffer.putShort(topicLength);
		offsetRequestBuffer.put(topicBytes);

		// Only requesting for 1 partition
		offsetRequestBuffer.putInt(1);

		// Partition
		offsetRequestBuffer.putInt(partition);

		// Time for offset
		offsetRequestBuffer.putLong(time);

		// We only need one offset
		offsetRequestBuffer.putInt(1);

		// Add the length to the start
		offsetRequestBuffer.putInt(0, offsetRequestBuffer.position() - 4);

		brokerOut.write(offsetRequestBytes, 0, offsetRequestBuffer.position());
	}

	private long getOffsetResponse(int correlationId) throws IOException
	{
		LOG.debug("[{}-{}] waiting for response. correlation id = {}", topic, partition, correlationId);

		try
		{
			// read the length of the response			
			bytesRead = 0;
			while (bytesRead < 4)
			{
				bytesRead += brokerIn.read(offsetResponseBytes, bytesRead, 4 - bytesRead);
			}
			
			offsetResponseBuffer.clear();
			responseLength = offsetResponseBuffer.getInt();

			bytesRead = 0;
			while (bytesRead < responseLength)
			{
				bytesRead += brokerIn.read(offsetResponseBytes, bytesRead, responseLength - bytesRead);
				LOG.debug("[{}-{}] read {} bytes", topic, partition, bytesRead);
			}
			
			offsetResponseBuffer.clear();

			// Check correlation Id
			responseCorrelationId = offsetResponseBuffer.getInt();
			
			if (responseCorrelationId != correlationId)
			{
				LOG.error("[{}-{}] correlation id mismatch.  Expected {}, got {}", topic, partition, correlationId, responseCorrelationId);
				throw new IOException("Correlation ID mismatch.  Expected " + correlationId + ". Got " + responseCorrelationId + ".");
			}

			// We can skip a bunch of stuff here.
			// There is 1 topic (4 bytes), then the topic name (2 + topicLength
			// bytes), then the number of partitions (which is 1) (4 bytes),
			// then the partition id (4 bytes)
			
			offsetResponseBuffer.position(offsetResponseBuffer.position() + 4 + 2 + topicLength + 4 + 4);

			// Next is the error code.
			errorCode = offsetResponseBuffer.getShort();

			if (errorCode == KafkaError.OffsetOutOfRange.getCode())
			{
				throw new OffsetOutOfRangeException();
			} 
			else
			{
				if (errorCode != KafkaError.NoError.getCode())
				{
					throw new IOException("Error from Kafka. (" + errorCode + ") " + KafkaError.getMessage(errorCode));
				}
			}

			// Finally, the offset. There is an array of one (skip 4 bytes)
			offsetResponseBuffer.position(offsetResponseBuffer.position() + 4);
			LOG.debug("Succeeded in request.  correlation id = {}", correlationId);
			
			return offsetResponseBuffer.getLong();

		} 
		catch (SocketTimeoutException e)
		{
			LOG.error("[{}-{}] socket timeout to {}: {}", topic, partition, broker.getNiceDescription(), e);
			connectToBroker();
			return -1;
		}
		finally
		{
			// Clean out any other data that is sitting on the socket to be  read. 
			// It's useless to us, but may through off future transactions if we
			// leave it there.
			
			bytesRead = 0;
			while (brokerIn.available() > 0)
			{
				bytesRead += brokerIn.read(offsetResponseBytes, bytesRead, offsetResponseBytes.length);
			}
		}
	}

	private void sendConsumeRequest(int correlationId) throws IOException
	{
		LOG.debug("[{}-{}] sending consume request. correlation id = {}", topic, partition, correlationId);

		requestBuffer.clear();

		// Skip 4 bytes for the request size
		requestBuffer.position(requestBuffer.position() + 4);

		// API key
		requestBuffer.putShort(Constants.APIKEY_FETCH_REQUEST);

		// API Version
		requestBuffer.putShort(Constants.API_VERSION);

		// Correlation Id
		requestBuffer.putInt(correlationId);

		// ClientId
		requestBuffer.putShort(clientIdLength);
		requestBuffer.put(clientIdBytes);

		// Replica ID is always -1
		requestBuffer.putInt(-1);

		// Max wait time
		requestBuffer.putInt(fetchWaitMaxMs);

		// Min bytes
		requestBuffer.putInt(fetchMinBytes);

		// Only requesting for 1 topic
		requestBuffer.putInt(1);

		// Topic Name
		requestBuffer.putShort(topicLength);
		requestBuffer.put(topicBytes);

		// Only requesting for 1 partition
		requestBuffer.putInt(1);

		// Partition
		requestBuffer.putInt(partition);

		// FetchOffset
		requestBuffer.putLong(offset);

		// MaxBytes
		requestBuffer.putInt(fetchMessageMaxBytes);

		/* Fill in missing data */
		// Full size
		requestBuffer.putInt(0, requestBuffer.position() - 4);

		// Send!
		brokerOut.write(requestBytes, 0, requestBuffer.position());
	}

	private void receiveConsumeResponse(int correlationId) throws IOException
	{
		LOG.debug("[{}-{}] waiting for response. correlation id = {}", topic, partition, correlationId);

		try
		{
			// read the length of the response			
			bytesRead = 0;			
			while (bytesRead < 4)
			{
				bytesRead += brokerIn.read(responseBytes, bytesRead, 4 - bytesRead);
			}
			
			responseBuffer.clear();
			responseLength = responseBuffer.getInt();
			bytesRead = 0;
			
			while (bytesRead < responseLength)
			{
				bytesRead += brokerIn.read(responseBytes, bytesRead, responseLength - bytesRead);
			}

			responseBuffer.clear();

			// Next is the corelation ID 

			responseCorrelationId = responseBuffer.getInt();
			
			if (responseCorrelationId != correlationId)
			{
				LOG.error("[{}-{}] correlation id mismatch.  Expected {}, got {}", topic, partition, correlationId, responseCorrelationId);
				throw new IOException("Correlation ID mismatch.  Expected " + correlationId + ". Got " + responseCorrelationId + ".");
			}

			// We can skip a bunch of stuff here.
			// There is 1 topic (4 bytes), then the topic name (2 + topicLength
			// bytes), then the number of partitions (which is 1) (4 bytes),
			// then the partition id (4 bytes)
			
			responseBuffer.position(responseBuffer.position() + 4 + 2 + topicLength + 4 + 4);

			// Next is the error code.
			
			errorCode = responseBuffer.getShort();

			if (errorCode == KafkaError.OffsetOutOfRange.getCode())
			{
				throw new OffsetOutOfRangeException();
			} 
			else
			{
				if (errorCode != KafkaError.NoError.getCode())
				{
					throw new IOException("Error from Kafka. (" + errorCode + ") " + KafkaError.getMessage(errorCode));
				}
			}

			// Next is the high watermark
			highWaterMark = responseBuffer.getLong();
			
			LOG.debug("[{}-{}] receiveConsumeRequest offset {} highWaterMark {}", topic, partition, offset, highWaterMark);

			// Message set size
			messageSetSize = responseBuffer.getInt();

			messageSetReader.init(responseBytes, responseBuffer.position(), messageSetSize);

			LOG.debug("[{}-{}] succeeded in request.  correlation id = {}", topic, partition, correlationId);

		} 
		finally
		{
			// Clean out any other data that is sitting on the socket to be read. 
			// It's  useless to us, but may through off future transactions if we
			// leave it there.
			
			bytesRead = 0;
			
			while (brokerIn.available() > 0)
			{
				bytesRead += brokerIn.read(responseBytes, bytesRead, responseBytes.length);
			}
		}
	}

	private void connectToBroker()
	{
		while (true)
		{
			try
			{
				MetaData meta = MetaData.getMetaData(conf.getMetadataBrokerList(), topic, clientId);
				broker = meta.getBroker(meta.getTopic(topic).getPartition(partition).getLeader());
				
				LOG.info("[{}-{}] connecting to broker {}", topic, partition, broker.getNiceDescription());
				
				brokerSocket = new Socket( broker.getHost(), broker.getPort());
				brokerSocket.setSoTimeout(conf.getSocketTimeoutMs());
				brokerSocket.setReceiveBufferSize(conf.getSocketReceiveBufferBytes());
				brokerIn = brokerSocket.getInputStream();
				brokerOut = brokerSocket.getOutputStream();
				
				LOG.info("[{}-{}] successfully connected to broker {} and set a timeout of {}", 
					 topic, partition, broker.getNiceDescription(), conf.getSocketTimeoutMs());

				break;
			} 
			catch (Exception e)
			{
				LOG.error("[{}-{}] error connecting to broker.", topic, partition, e);
				try
				{
					Thread.sleep(100);
				} 
				catch (InterruptedException e1) { }
			}
		}
	}

	public long getLastOffset()
	{
		return lastOffset;
	}

	public long getNextOffset()
	{
		return offset;
	}

	public long getHighWaterMark()
	{
		return highWaterMark;
	}
	
	public void setNextOffset(long nextOffset) throws IOException
	{
		LOG.info("[{}-{}] request to set the next offset to {} received", topic, partition, nextOffset);
		
		this.offset = nextOffset;
		
		correlationId++;

		try
		{
			sendConsumeRequest(correlationId);
			receiveConsumeResponse(correlationId);
		}
		catch (SocketTimeoutException e)
		{
			LOG.error("[{}-{}] socket timeout to {}", topic, partition, broker.getNiceDescription());
			connectToBroker();
		}
		
		LOG.info("[{}-{}] successfully set the next offset to {} via correlation ID {}", topic, partition, nextOffset, correlationId);
	}

}
