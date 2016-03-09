/**
 * Copyright 2015 BlackBerry, Limited.
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
package com.blackberry.bdp.krackle.auth;

import com.blackberry.bdp.krackle.exceptions.MissingConfigurationException;
import com.blackberry.bdp.krackle.exceptions.InvalidConfigurationTypeException;
import com.blackberry.bdp.krackle.exceptions.AuthenticationException;
import com.blackberry.bdp.krackle.jaas.Login;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Properties;
import javax.security.auth.login.LoginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticatedSocketSingleton {

	private static final Logger LOG = LoggerFactory.getLogger(AuthenticatedSocketSingleton.class);

	public static enum Protocol {
		PLAINTEXT,
		SSL,
		SASL_PLAINTEXT,
		SASL_SSL
	}

	private Protocol kafkaSecurityProtocol;
	private HashMap<String, Object> configuration;

	private AuthenticatedSocketSingleton() {
		kafkaSecurityProtocol = Protocol.PLAINTEXT;
		configuration = new HashMap<>();
	}

	private static class SingletonHolder {
		public static final AuthenticatedSocketSingleton INSTANCE = new AuthenticatedSocketSingleton();
	}

	public static AuthenticatedSocketSingleton getInstance() {
		return SingletonHolder.INSTANCE;
	}


	public void configure(Properties props)
		 throws AuthenticationException, LoginException {
		configuration = new HashMap<>();

		kafkaSecurityProtocol = AuthenticatedSocketSingleton.Protocol.valueOf(
			 props.getProperty("kafka.security.protocol", "PLAINTEXT").trim().toUpperCase());

		switch (kafkaSecurityProtocol) {
			case PLAINTEXT:
				break;
			case SASL_PLAINTEXT:
				Login login = new Login(
					 props.getProperty("kafka.client.jaas.login.context", "kafkaClient").trim(),
					 new Login.ClientCallbackHandler());
				login.startThreadIfNeeded();
				configuration.put("subject", login.getSubject());
				configuration.put("clientPrincipal", login.getPrincipal());
				configuration.put("servicePrincipal", props.getProperty(
					 "kafka.security.protocol.service.principal", "kafka").trim());
				break;
			default:
				throw new AuthenticationException(String.format(
					 "kafka.security.protocol=%s not recognized or supported",
					 kafkaSecurityProtocol));
		}
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
			switch (kafkaSecurityProtocol) {
				case PLAINTEXT:
					PlainTextAuthenticator pta = new PlainTextAuthenticator(socket);
					return pta.getSocket();
				case SASL_PLAINTEXT:
					SaslPlainTextAuthenticator spta = new SaslPlainTextAuthenticator(socket);
					spta.configure(configuration);
					spta.authenticate();
					LOG.info("sasl authenticated socket has been created");
					return socket;
				default:
					throw new AuthenticationException(
						 String.format("%s not supported", kafkaSecurityProtocol));
			}
		} catch (IOException | MissingConfigurationException | InvalidConfigurationTypeException | AuthenticationException e) {
			LOG.error("failed to build socket to {}: ",
				 socket.getInetAddress().getHostName(),
				 e);
			throw new AuthenticationException(
				 String.format("failed to a authenticate %s", e.getMessage()));

		}
	}

	/**
	 * @return the kafkaSecurityProtocol
	 */
	public AuthenticatedSocketSingleton.Protocol getKafkaSecurityProtocol() {
		return kafkaSecurityProtocol;
	}

}
