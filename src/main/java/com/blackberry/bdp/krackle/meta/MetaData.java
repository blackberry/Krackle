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
package com.blackberry.bdp.krackle.meta;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.bdp.krackle.Constants;
import com.blackberry.bdp.krackle.auth.AuthenticatedSocketBuilder;
import com.blackberry.bdp.krackle.auth.AuthenticationException;

/**
 * Gather and store metadata for a topic.
 *
 * Contents are
 * <ul>
 * <li>brokers: a map of broker id to Broker object</li>
 * <li>topics: a map of topic names to Topic objects</li>
 * </ul>
 */
public class MetaData {

	private static final Logger LOG = LoggerFactory.getLogger(MetaData.class);
	private static final Charset UTF8 = Charset.forName("UTF-8");

	private final Map<Integer, Broker> brokers = new HashMap<>();
	private final Map<String, Topic> topics = new HashMap<>();
	private int correlationId;

	/**
	 * New instance, with the list of seed brokers represented a List of host:port
	 * entries.
	 *
	 * @param authSocketBuilder
	 * @param config
	 * @param metadataBrokerList
	 * @param topicString topic to get metadata about.
	 * @param clientIdString clientId to send with request.
	 * @return a new MetaData object containing information on the topic.
	 */
	public static MetaData getMetaData(AuthenticatedSocketBuilder authSocketBuilder,
		 List<String> metadataBrokerList,
		 String topicString, String clientIdString) {
		LOG.info("Getting metadata for {}", topicString);

		MetaData metadata = new MetaData();
		metadata.setCorrelationId((int) System.currentTimeMillis());

		// Get the broker seeds from the config.
		List<HostAndPort> seedBrokers = new ArrayList<>();
		for (String hnp : metadataBrokerList) {
			String[] hostPort = hnp.split(":", 2);
			try {
				for(InetAddress curhost: InetAddress.getAllByName(hostPort[0])) {
					seedBrokers.add(new HostAndPort(curhost, Integer.parseInt(hostPort[1])));
					LOG.debug("Adding Broker Candidate - {}", curhost);
				}
			} catch (UnknownHostException e) {
				LOG.info("Unknown Host: {}", hostPort[0]);
			}
		}

		// Try each seed broker in a random order
		Collections.shuffle(seedBrokers);

		Socket sock = null;
		for (HostAndPort hnp : seedBrokers) {
			try {
				sock = authSocketBuilder.build(hnp.host, hnp.port);
				sock.setSoTimeout(5000);
			} catch (AuthenticationException  e) {
				LOG.warn("authentication exception: {}", hnp.host);
				continue;
			} catch (IOException e) {
				LOG.warn("Error connecting to {}:{}", hnp.host, hnp.port);
				continue;
			}
			break;
		}

		if (sock == null) {
			LOG.error("Unable to connect to any seed broker to updata metadata.");
			return null;
		}

		try {
			sock.getOutputStream().write(
				 buildMetadataRequest(topicString.getBytes(UTF8),
					  clientIdString.getBytes(UTF8), metadata.getCorrelationId()));

			byte[] sizeBuffer = new byte[4];
			InputStream in = sock.getInputStream();
			int bytesRead = 0;
			while (bytesRead < 4) {
				int read = in.read(sizeBuffer, bytesRead, 4 - bytesRead);
				if (read == -1) {
					throw new IOException(
						 "Stream ended before data length could be read.");
				}
				bytesRead += read;
			}
			int length = ByteBuffer.wrap(sizeBuffer).getInt();

			byte[] responseArray = new byte[length];
			bytesRead = 0;
			while (bytesRead < length) {
				int read = in.read(responseArray, bytesRead, length - bytesRead);
				if (read == -1) {
					throw new IOException("Stream ended before end of response.");
				}
				bytesRead += read;
			}
			ByteBuffer responseBuffer = ByteBuffer.wrap(responseArray);
			int cid = responseBuffer.getInt();
			if (cid != metadata.getCorrelationId()) {
				LOG.error("Got back wrong correlation id.");
				return null;
			}

			// Load the brokers
			int numBrokers = responseBuffer.getInt();
			for (int i = 0; i < numBrokers; i++) {
				int nodeId = responseBuffer.getInt();
				String host = readString(responseBuffer);
				int port = responseBuffer.getInt();

				metadata.getBrokers().put(nodeId, new Broker(nodeId, host, port));

				LOG.debug("Broker {} @ {}:{}", nodeId, host, port);
			}

			// Load the topics
			int numTopics = responseBuffer.getInt();
			for (int i = 0; i < numTopics; i++) {

				short errorCode = responseBuffer.getShort();
				String name = readString(responseBuffer);
				Topic t = new Topic(name);
				LOG.debug("Topic {} (Error {})", name, errorCode);

				int numParts = responseBuffer.getInt();
				for (int j = 0; j < numParts; j++) {

					short partError = responseBuffer.getShort();
					int partId = responseBuffer.getInt();
					int leader = responseBuffer.getInt();
					LOG.debug("    Partition ID={}, Leader={} (Error={})", partId,
						 leader, partError);

					Partition part = new Partition(partId);
					part.setLeader(leader);

					int numReplicas = responseBuffer.getInt();
					for (int k = 0; k < numReplicas; k++) {
						int rep = responseBuffer.getInt();
						LOG.debug("        Replica on {}", rep);
						part.getReplicas().add(rep);
					}

					int numIsr = responseBuffer.getInt();
					for (int k = 0; k < numIsr; k++) {
						int isr = responseBuffer.getInt();
						LOG.debug("        Isr on {}", isr);
						part.getInSyncReplicas().add(isr);
					}

					t.getPartitions().add(part);
				}

				metadata.getTopics().put(name, t);
			}

		} catch (IOException e) {
			LOG.error("Failed to get metadata");
			return null;
		}

		LOG.info("Metadata request successful");
		return metadata;
	}

	private static String readString(ByteBuffer bb) {
		short length = bb.getShort();
		byte[] a = new byte[length];
		bb.get(a);
		return new String(a, UTF8);
	}

	private static byte[] buildMetadataRequest(byte[] topic, byte[] clientId,
		 int correlationId) {
		byte[] request = new byte[20 + clientId.length + topic.length];
		ByteBuffer bb = ByteBuffer.wrap(request);
		bb.putInt(16 + clientId.length + topic.length);
		bb.putShort(Constants.APIKEY_METADATA);
		bb.putShort(Constants.API_VERSION);
		bb.putInt(correlationId);
		bb.putShort((short) clientId.length);
		bb.put(clientId);

		// topics.
		bb.putInt(1);
		bb.putShort((short) topic.length);
		bb.put(topic);

		return request;
	}

  // We're storing the given hostname and not an InetAddress since we want to
	// re-resolve the address each time. This way changes to DNS can make us
	// change properly.
	private static class HostAndPort {

		InetAddress host;
		int port;

		HostAndPort(InetAddress host, int port) {
			this.host = host;
			this.port = port;
		}

		@Override
		public String toString() {
			return "HostAndPort [host=" + host.getHostName() + ", port=" + port + "]";
		}

	}

	public Map<Integer, Broker> getBrokers() {
		return brokers;
	}

	public Broker getBroker(Integer id) {
		return brokers.get(id);
	}

	public Map<String, Topic> getTopics() {
		return topics;
	}

	public Topic getTopic(String name) {
		return topics.get(name);
	}

	public int getCorrelationId() {
		return correlationId;
	}

	public void setCorrelationId(int correlationId) {
		this.correlationId = correlationId;
	}

	@Override
	public String toString() {
		return "MetaData [brokers=" + brokers + ", topics=" + topics + "]";
	}

}
