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
		SASL_PLAINTEXT,
		SASL_SSL
	}

	private final Protocol protocol;
	private final Map<String, Object> configs;

	private static final Logger LOG = LoggerFactory.getLogger(
		 AuthenticatedSocketBuilder.class);

	public AuthenticatedSocketBuilder(Protocol protocol,
		 Map<String, Object> configs) {
		this.protocol = protocol;
		this.configs = configs;
	}

	public Socket build(InetAddress host, int port)
		 throws AuthenticationException {
		try {
			Socket socket = new Socket(host, port);
			return build(socket);
		} catch (IOException | AuthenticationException e) {
			throw (AuthenticationException) e;
		}
	}

	public Socket build(String hostname, int port)
		 throws AuthenticationException {
		try {
			Socket socket = new Socket(hostname, port);
			return build(socket);
		} catch (IOException | AuthenticationException e) {
			throw (AuthenticationException) e;
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
					LOG.info("{}", socket.getInetAddress().getHostName());
					configs.put("hostname", socket.getInetAddress().getHostName());
					spta.configure(configs);
					spta.authenticate();
					return spta.getSocket();
				default:
					throw new AuthenticationException(
						 String.format("%s not supported", protocol));
			}
		} catch (IOException
			 | MissingConfigurationException
			 | InvalidConfigurationTypeException
			 | AuthenticationException e) {
			LOG.info("an {} exception occured {}: ",
				 e.getClass().getCanonicalName(),
				 e.getMessage(),
				 e);
			throw (AuthenticationException) e;
		}
	}
}
