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
import com.blackberry.bdp.krackle.exceptions.AuthenticationException;
import java.util.Arrays;

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
	private List<String> metadataBrokerList;
	private byte[] clientId;

	/**
	 * Metadata for a single topic with a string of seed brokers for a given client
	 *
	 * @param authSocketBuilder
	 * @param metadataBrokerString
	 * @param topic topic to get metadata about.
	 * @param clientIdString clientId to send with request.
	 * @return a new MetaData object containing information on the topic.
	 */
	public static MetaData getMetaData(AuthenticatedSocketBuilder authSocketBuilder,
		 String metadataBrokerString,
		 String topic,
		 String clientIdString) {
		return getMetaData(authSocketBuilder,
			 Arrays.asList(metadataBrokerString.split(",")),
			 topic,
			 clientIdString);
	}

	/**
	 * Metadata for a single topic with a list of seed brokers for a given client
	 *
	 * @param authSocketBuilder
	 * @param metadataBrokerList
	 * @param topic topic to get metadata about.
	 * @param clientIdString clientId to send with request.
	 * @return a new MetaData object containing information on the topic.
	 */
	public static MetaData getMetaData(AuthenticatedSocketBuilder authSocketBuilder,
		 List<String> metadataBrokerList,
		 String topic,
		 String clientIdString) {
		LOG.info("Getting metadata for {}", topic);

		MetaData metadata = new MetaData();
		metadata.metadataBrokerList = metadataBrokerList;
		metadata.correlationId = (int) System.currentTimeMillis();
		metadata.clientId = clientIdString.getBytes(UTF8);

		return getMetaData(metadata,
			 buildMetadataRequest(metadata, topic.getBytes(UTF8)),
			 authSocketBuilder);
	}

	/**
	 * Metadata for all topics with a string of seed brokers for a given client
	 *
	 * @param authSocketBuilder
	 * @param metadataBrokerString
	 * @param clientIdString clientId to send with request.
	 * @return a new MetaData object containing information on the topic.
	 */
	public static MetaData getMetaData(AuthenticatedSocketBuilder authSocketBuilder,
		 String metadataBrokerString,
		 String clientIdString) {

		return getMetaData(authSocketBuilder,
			 Arrays.asList(metadataBrokerString.split(",")),
			 clientIdString);
	}

	/**
	 * Metadata for all topics with a list of seed brokers for a given client
	 *
	 * @param authSocketBuilder
	 * @param metadataBrokerList
	 * @param clientIdString clientId to send with request.
	 * @return a new MetaData object containing information on the topic.
	 */
	public static MetaData getMetaData(AuthenticatedSocketBuilder authSocketBuilder,
		 List<String> metadataBrokerList,
		 String clientIdString) {
		LOG.info("Getting metadata for all topics");

		MetaData metadata = new MetaData();
		metadata.metadataBrokerList = metadataBrokerList;
		metadata.correlationId = (int) System.currentTimeMillis();
		metadata.clientId = clientIdString.getBytes(UTF8);

		return getMetaData(metadata,
			 buildMetadataRequest(metadata),
			 authSocketBuilder);
	}

	private static MetaData getMetaData(MetaData metadata,
		 byte[] request, AuthenticatedSocketBuilder authSocketBuilder) {

		// Get the broker seeds from the config.
		List<HostAndPort> seedBrokers = new ArrayList<>();
		for (String hnp : metadata.metadataBrokerList) {
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
			sock.getOutputStream().write(request);
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
					int leaderId = responseBuffer.getInt();
					LOG.debug("    Partition ID={}, Leader={} (Error={})", partId,
						 leaderId, partError);

					Partition part = new Partition(partId);
					part.setLeader(metadata.brokers.get(leaderId));

					int numReplicas = responseBuffer.getInt();
					for (int k = 0; k < numReplicas; k++) {
						int replicaBrokerId = responseBuffer.getInt();
						LOG.debug("        Replica on {}", replicaBrokerId);
						part.getReplicas().add(metadata.brokers.get(replicaBrokerId));
					}

					int numIsr = responseBuffer.getInt();
					for (int k = 0; k < numIsr; k++) {
						int isrBrokerId = responseBuffer.getInt();
						LOG.debug("        Isr on {}", isrBrokerId);
						part.getInSyncReplicas().add(metadata.brokers.get(isrBrokerId));
					}

					t.getPartitions().add(part);
				}

				metadata.getTopics().put(name, t);
			}

		} catch (IOException e) {
			LOG.error("Failed to get metadata");
			return null;
		} finally {
			try {
				sock.close();
			} catch (IOException ioe) {
				LOG.error("failed to close socket: ", ioe);
			}
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

	private static byte[] buildMetadataRequest(MetaData md) {
		byte[] request = new byte[20 + md.clientId.length];
		ByteBuffer bb = ByteBuffer.wrap(request);
		bb.putInt(16 + md.clientId.length);
		bb.putShort(Constants.APIKEY_METADATA);
		bb.putShort(Constants.API_VERSION);
		bb.putInt(md.correlationId);
		bb.putShort((short) md.clientId.length);
		bb.put(md.clientId);
		bb.putInt(0);
		return request;
	}

	private static byte[] buildMetadataRequest(MetaData md, byte[] topic) {
		byte[] request = new byte[20 + md.clientId.length + topic.length];
		ByteBuffer bb = ByteBuffer.wrap(request);
		bb.putInt(16 + md.clientId.length + topic.length);
		bb.putShort(Constants.APIKEY_METADATA);
		bb.putShort(Constants.API_VERSION);
		bb.putInt(md.correlationId);
		bb.putShort((short) md.clientId.length);
		bb.put(md.clientId);
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

	@Override
	public String toString() {
		return "MetaData [brokers=" + brokers + ", topics=" + topics + "]";
	}

}
