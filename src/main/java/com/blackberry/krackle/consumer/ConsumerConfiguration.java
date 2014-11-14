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

package com.blackberry.krackle.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for a consumer.
 *
 * Many of these properties are the same as those in the standard Java client, as documented at http://kafka.apache.org/documentation.html#consumerconfigs
 *
 * Valid properties are
 * <table>
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
 * <td>(required) A list of seed brokers to connect to in order to get information about the Kafka broker cluster.</td>
 * </tr>
 *
 * <tr>
 * <td>fetch.message.max.bytes</td>
 * <td>1024 * 1024</td>
 * <td>The number of byes of messages to attempt to fetch for each topic-partition in each fetch request. These bytes will be read into memory for each partition, so this helps control the memory used by the consumer. The fetch request size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch.</td>
 * </tr>
 *
 * <tr>
 * <td>fetch.wait.max.ms</td>
 * <td>100</td>
 * <td>The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes</td>
 * </tr>
 *
 * <tr>
 * <td>fetch.min.bytes</td>
 * <td>1</td>
 * <td>The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will wait for that much data to accumulate before answering the request.</td>
 * </tr>
 *
 * <tr>
 * <td>socket.receive.buffer.bytes</td>
 * <td>64 * 1024</td>
 * <td>The socket receive buffer for network requests</td>
 * </tr>
 *
 * <tr>
 * <td>auto.offset.reset</td>
 * <td>largest</td>
 * <td>What to do when there is no initial offset in ZooKeeper or if an offset is out of range:
 * <ul>
 * <li>smallest : automatically reset the offset to the smallest offset</li>
 * <li>largest : automatically reset the offset to the largest offset</li>
 * <li>anything else: throw exception to the consumer</li>
 * </ul>
 * </td>
 * </tr>
 *
 * </table>
 */
public class ConsumerConfiguration
{
	private static final Logger LOG = LoggerFactory.getLogger(ConsumerConfiguration.class);

	private List<String> metadataBrokerList;
	private int fetchMessageMaxBytes;
	private int fetchWaitMaxMs;
	private int fetchMinBytes;
	private int socketReceiveBufferBytes;
	private String autoOffsetReset;
	private int socketTimeoutMs;

	/**
	 * Creates a new configuration from a given Properties object.
	 *
	 * @param props Properties to build configuration from.
	 * @throws Exception
	 */
	
	public ConsumerConfiguration(Properties props) throws Exception
	{
		LOG.info("Building configuration.");

		metadataBrokerList = new ArrayList<String>();
		String metadataBrokerListString = props.getProperty("metadata.broker.list");
		
		if (metadataBrokerListString == null || metadataBrokerListString.isEmpty())
		{
			throw new Exception("metadata.broker.list cannot be empty.");
		}
		
		for (String s : metadataBrokerListString.split(","))
		{
			// This is not a good regex. Could make it better.
			if (s.matches("^[\\.a-zA-Z0-9-]*:\\d+$"))
			{
				metadataBrokerList.add(s);
			} 
			else
			{
				throw new Exception(
					 "metata.broker.list must contain a list of hosts and ports (localhost:123,192.168.1.1:456).  Got "
					 + metadataBrokerListString);
			}
		}
		
		LOG.info("metadata.broker.list = {}", metadataBrokerList);

		fetchMessageMaxBytes = Integer.parseInt(props.getProperty("fetch.message.max.bytes", "" + (1024 * 1024)));
		
		if (fetchMessageMaxBytes <= 0)
		{
			throw new Exception("fetch.message.max.bytes must be positive.");
		}
		
		LOG.info("fetch.message.max.bytes = {}", fetchMessageMaxBytes);

		fetchWaitMaxMs = Integer.parseInt(props.getProperty("fetch.wait.max.ms", "100"));
		
		if (fetchWaitMaxMs < 0)
		{
			throw new Exception("fetch.wait.max.ms cannot be negative.");
		}
		
		LOG.info("fetch.wait.max.ms = {}", fetchWaitMaxMs);

		fetchMinBytes = Integer.parseInt(props.getProperty("fetch.min.bytes", "1"));
		
		if (fetchMinBytes < 0)
		{
			throw new Exception("fetch.min.bytes cannot be negative.");
		}
		
		LOG.info("fetch.min.bytes = {}", fetchMinBytes);

		socketReceiveBufferBytes = Integer.parseInt(props.getProperty("socket.receive.buffer.bytes", "" + (64 * 1024)));
		
		if (socketReceiveBufferBytes < 0)
		{
			throw new Exception("socket.receive.buffer.bytes must be positive.");
		}
		
		LOG.info("socket.receive.buffer.bytes = {}", socketReceiveBufferBytes);

		autoOffsetReset = props.getProperty("auto.offset.reset", "largest");
		
		LOG.info("auto.offset.reset = {}", autoOffsetReset);
		
		socketTimeoutMs = Integer.parseInt(props.getProperty("socket.timeout.seconds", "" + (30 * 1000)));
		
		if (getSocketTimeoutMs() < 0)
		{
			throw new Exception("socket.timeout.seconds must be positive.");
		}

	}

	public List<String> getMetadataBrokerList()
	{
		return metadataBrokerList;
	}

	public void setMetadataBrokerList(List<String> metadataBrokerList)
	{
		this.metadataBrokerList = metadataBrokerList;
	}

	public int getFetchMessageMaxBytes()
	{
		return fetchMessageMaxBytes;
	}

	public void setFetchMessageMaxBytes(int fetchMessageMaxBytes)
	{
		this.fetchMessageMaxBytes = fetchMessageMaxBytes;
	}

	public int getFetchWaitMaxMs()
	{
		return fetchWaitMaxMs;
	}

	public void setFetchWaitMaxMs(int fetchWaitMaxMs)
	{
		this.fetchWaitMaxMs = fetchWaitMaxMs;
	}

	public int getFetchMinBytes()
	{
		return fetchMinBytes;
	}

	public void setFetchMinBytes(int fetchMinBytes)
	{
		this.fetchMinBytes = fetchMinBytes;
	}

	public int getSocketReceiveBufferBytes()
	{
		return socketReceiveBufferBytes;
	}

	public void setSocketReceiveBufferBytes(int socketReceiveBufferBytes)
	{
		this.socketReceiveBufferBytes = socketReceiveBufferBytes;
	}

	public String getAutoOffsetReset()
	{
		return autoOffsetReset;
	}

	public void setAutoOffsetReset(String autoOffsetReset)
	{
		this.autoOffsetReset = autoOffsetReset;
	}

	/**
	 * @return the socketTimeoutMs
	 */
	public int getSocketTimeoutMs()
	{
		return socketTimeoutMs;
	}

	/**
	 * @param socketTimeoutMs the socketTimeoutMs to set
	 */
	public void setSocketTimeoutMs(int socketTimeoutMs)
	{
		this.socketTimeoutMs = socketTimeoutMs;
	}

}
