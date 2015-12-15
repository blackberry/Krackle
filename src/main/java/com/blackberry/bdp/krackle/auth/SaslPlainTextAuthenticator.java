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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;

import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaslPlainTextAuthenticator implements Authenticator{

	public enum SaslState {

		INITIAL, INTERMEDIATE, COMPLETE, FAILED

	}

	private static final Logger LOG = LoggerFactory.getLogger(SaslPlainTextAuthenticator.class);

	// Configurable Items
	private String hostname;
	private Subject subject;
	private String servicePrincipal;

	private SaslClient saslClient;
	private String clientPrincipalName;
	private boolean configured;

	private final Socket socket;
	private final DataInputStream inStream;
	private final DataOutputStream outStream;

	private static final byte[] EMPTY = new byte[0];

	private SaslState saslState;

	/**
	 * Will create a socket based on host name and port
	 * @param hostname
	 * @param port
	 * @throws IOException
	 * @throws SaslException
	 */
	public SaslPlainTextAuthenticator(String hostname, int port)
		 throws IOException, SaslException {
		this(new Socket(hostname, port));
	}

	/**
	 * Will use an existing socket
	 * @param socket
	 * @throws IOException
	 * @throws SaslException
	 */
	public SaslPlainTextAuthenticator(Socket socket)
		 throws IOException, SaslException {
		this.socket = socket;
		this.inStream = new DataInputStream(socket.getInputStream());
		this.outStream = new DataOutputStream(socket.getOutputStream());
		this.configured = false;
		saslState = SaslState.INITIAL;
	}

	@Override
	public void configure(Map<String, ?> configs)
		 throws MissingConfigurationException,
		 InvalidConfigurationTypeException,
		 SaslException {

		if (!configs.containsKey("subject")) {
			throw new MissingConfigurationException("`subject` not defined in configration");
		} else if (!configs.get("subject").getClass().equals(Subject.class)) {
			String type = Subject.class.getCanonicalName();
			throw new InvalidConfigurationTypeException("`subject` is not a " + type);
		} else {
			subject = (Subject) configs.get("subject");
		}

		if (!configs.containsKey("servicePrincipal")) {
			throw new MissingConfigurationException("`servicePrincipal` not defined in configration");
		} else if (!configs.get("servicePrincipal").getClass().equals(String.class)) {
			String type = String.class.getCanonicalName();
			throw new InvalidConfigurationTypeException("`servicePrincipal` is not a " + type);
		} else {
			servicePrincipal = (String) configs.get("servicePrincipal");
		}

		if (!configs.containsKey("hostname")) {
			throw new MissingConfigurationException("`hostname` not defined in configration");
		} else if (!configs.get("hostname").getClass().equals(String.class)) {
			String type = String.class.getCanonicalName();
			throw new InvalidConfigurationTypeException("`hostname` is not a " + type);
		} else {
			hostname = (String) configs.get("hostname");
		}

		Principal clientPrincipal = subject.getPrincipals().iterator().next();
		this.clientPrincipalName = clientPrincipal.getName();
		this.saslClient = createSaslClient();
		configured = true;
	}

	private SaslClient createSaslClient() throws SaslException {
		try {
			return Subject.doAs(subject, new PrivilegedExceptionAction<SaslClient>() {
				@Override
				public SaslClient run() throws SaslException {
					String[] mechs = {"GSSAPI"};
					LOG.info("Creating SaslClient: client={}; service={}; serviceHostname={}; mechs={}",
						 clientPrincipalName, servicePrincipal, hostname, Arrays.toString(mechs));
					return Sasl.createSaslClient(mechs, clientPrincipalName, servicePrincipal, hostname, null,
						 new ClientCallbackHandler());
				}

			});
		} catch (PrivilegedActionException e) {
			throw new SaslException("Failed to create SaslClient", e.getCause());
		}
	}

	/**
	 * Sends an empty message to the server to initiate the authentication process. It then evaluates server challenges
	 * via `SaslClient.evaluateChallenge` and returns client responses until authentication succeeds or fails.
	 *
	 * The messages are sent and received as size delimited bytes that consists of a 4 byte network-ordered size N
	 * followed by N bytes representing the opaque payload.
	 * @throws java.io.IOException
	 */
	@Override
	public void authenticate() throws IOException {
		if (!configured) {
			throw new IOException("authentication attempted on unconfigured authenticator");
		}
		byte[] challenge;
		while (!saslClient.isComplete()) {
			switch (saslState) {
				case INITIAL:
					sendSaslToken(EMPTY);
					saslState = SaslState.INTERMEDIATE;
					break;
				case INTERMEDIATE:
					int length = inStream.readInt();
					challenge = new byte[length];
					inStream.readFully(challenge);
					sendSaslToken(challenge);
					if (saslClient.isComplete()) {
						saslState = SaslState.COMPLETE;
					}
					break;
				case COMPLETE:
					break;
				case FAILED:
					throw new IOException("SASL handshake failed");
			}
		}
	}

	private void sendSaslToken(byte[] serverToken) throws IOException {
		if (!saslClient.isComplete()) {
			try {
				byte[] saslToken = createSaslToken(serverToken);
				if (saslToken != null) {
					outStream.writeInt(saslToken.length);
					outStream.write(saslToken);
					outStream.flush();
				}
			} catch (IOException e) {
				saslState = SaslState.FAILED;
				throw e;
			}
		} else {
			LOG.warn("attempting to send sasl token to a completed sasl client");
		}
	}

	private byte[] createSaslToken(final byte[] saslToken) throws SaslException {
		if (saslToken == null) {
			throw new SaslException("Error authenticating with the Kafka Broker: received a nul saslToken.");
		}
		try {
			return Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
				@Override
				public byte[] run() throws SaslException {
					return saslClient.evaluateChallenge(saslToken);
				}
			});
		} catch (PrivilegedActionException e) {
			String error = "An error: (" + e + ") occurred when evaluating SASL token received from the Kafka Broker.";
			// Try to provide hints to use about what went wrong so they can fix their configuration.
			// TODO: introspect about e: look for GSS information.
			final String unknownServerErrorText
				 = "(Mechanism level: Server not found in Kerberos database (7) - UNKNOWN_SERVER)";
			if (e.toString().contains(unknownServerErrorText)) {
				error += " This may be caused by Java's being unable to resolve the Kafka Broker's"
					 + " hostname correctly. You may want to try to adding"
					 + " '-Dsun.net.spi.nameservice.provider.1=dns,sun' to your client's JVMFLAGS environment."
					 + " Users must configure FQDN of kafka brokers when authenticating using SASL and"
					 + " `socketChannel.socket().getInetAddress().getHostName()` must match the hostname in `principal/hostname@realm`";
			}
			error += " Kafka Client will go to AUTH_FAILED state.";
			//Unwrap the SaslException inside `PrivilegedActionException`
			throw new SaslException(error, e.getCause());
		}
	}

	private static class ClientCallbackHandler implements CallbackHandler {

		@Override
		public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
			for (Callback callback : callbacks) {
				LOG.info("callback {} received", callback.toString());
				if (callback instanceof NameCallback) {
					NameCallback nc = (NameCallback) callback;
					nc.setName(nc.getDefaultName());
				} else {
					if (callback instanceof PasswordCallback) {
						// Call `setPassword` once we support obtaining a password from the user and update message below
						throw new UnsupportedCallbackException(callback, "Could not login: the client is being asked for a password, but the Kafka"
							 + " client code does not currently support obtaining a password from the user."
							 + " Make sure -Djava.security.auth.login.config property passed to JVM and"
							 + " the client is configured to use a ticket cache (using"
							 + " the JAAS configuration setting 'useTicketCache=true)'. Make sure you are using"
							 + " FQDN of the Kafka broker you are trying to connect to.");
					} else {
						if (callback instanceof RealmCallback) {
							RealmCallback rc = (RealmCallback) callback;
							rc.setText(rc.getDefaultText());
						} else {
							if (callback instanceof AuthorizeCallback) {
								AuthorizeCallback ac = (AuthorizeCallback) callback;
								String authId = ac.getAuthenticationID();
								String authzId = ac.getAuthorizationID();
								ac.setAuthorized(authId.equals(authzId));
								if (ac.isAuthorized()) {
									ac.setAuthorizedID(authzId);
								}
							} else {
								throw new UnsupportedCallbackException(callback, "Unrecognized SASL ClientCallback");
							}
						}
					}
				}
			}
		}

	}

	@Override
	public boolean complete() {
		return saslState == SaslState.COMPLETE;
	}

	@Override
	public void close() throws IOException {
		saslClient.dispose();
	}

	@Override
	public Socket getSocket() {
		return socket;
	}

	@Override
	public boolean configured() {
		return configured;
	}

}
