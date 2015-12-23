/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.blackberry.bdp.krackle.auth;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticatedSocketBuilder {

	public static enum Protocol {
		PLAINTEXT,
		SSL,
		SASL_PLAINTEXT,
		SASL_SSL
	}

	private final Protocol protocol;
	private Map<String, Object> securityConfigs;

	private static final Logger LOG = LoggerFactory.getLogger(
		 AuthenticatedSocketBuilder.class);

	public AuthenticatedSocketBuilder(String protocolString,
		 Map<String, Object> securityConfigs) throws AuthenticationException {
		this.securityConfigs = securityConfigs;
		switch (protocolString.trim().toUpperCase()) {
			case "PLAINTEXT":
				protocol = Protocol.PLAINTEXT;
				break;
			case "SSL":
				protocol = Protocol.SSL;
				break;
			case "SASL_PLAINTEXT":
				protocol = Protocol.SASL_PLAINTEXT;
				break;
			case "SASL_SSL":
				protocol = Protocol.SASL_SSL;
				break;
			default:
				throw new AuthenticationException(String.format(
					 "protocol %s not recognized", protocolString));
		}
	}

	public AuthenticatedSocketBuilder(Protocol protocol,
		 Map<String, Object> securityConfigs) {
		this.protocol = protocol;
		this.securityConfigs = securityConfigs;
	}

	public Socket build(InetAddress host, int port)
		 throws AuthenticationException {
		try {
			Socket socket = new Socket(host, port);
			return build(socket);
		} catch (IOException | AuthenticationException e) {
			LOG.error("an {} exception occured {}: ",
				 e.getClass().getCanonicalName(),
				 e.getMessage(),
				 e);
			throw new AuthenticationException(
				 String.format("failed to a authenticate %s", e.getMessage()));
		}
	}

	public Socket build(String hostname, int port)
		 throws AuthenticationException {
		try {
			Socket socket = new Socket(hostname, port);
			return build(socket);
		} catch (IOException | AuthenticationException e) {
			LOG.error("an {} exception occured {}: ",
				 e.getClass().getCanonicalName(),
				 e.getMessage(),
				 e);
			throw new AuthenticationException(
				 String.format("failed to a authenticate %s", e.getMessage()));
		}
	}

	public Socket build(Socket socket) throws AuthenticationException {
		try {
			switch (protocol) {
				case PLAINTEXT:
					PlainTextAuthenticator pta
						 = new PlainTextAuthenticator(socket);
					return pta.getSocket();
				case SASL_PLAINTEXT:
					SaslPlainTextAuthenticator spta
						 = new SaslPlainTextAuthenticator(socket);
					LOG.debug("using remote hostname {}",
						 socket.getInetAddress().getHostName());
					securityConfigs.put("hostname", socket.getInetAddress().getHostName());
					spta.configure(securityConfigs);
					spta.authenticate();
					LOG.info("sasl authenticated socket has been created");
					return spta.getSocket();
				default:
					throw new AuthenticationException(
						 String.format("%s not supported", protocol));
			}
		} catch (IOException
			 | MissingConfigurationException
			 | InvalidConfigurationTypeException
			 | AuthenticationException e) {
			LOG.error("an {} exception occured {}: ",
				 e.getClass().getCanonicalName(),
				 e.getMessage(),
				 e);
			throw new AuthenticationException(
				 String.format("failed to a authenticate %s", e.getMessage()));

		}
	}

}
