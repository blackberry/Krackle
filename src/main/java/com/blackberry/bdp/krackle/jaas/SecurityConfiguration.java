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
package com.blackberry.bdp.krackle.jaas;

import com.blackberry.bdp.krackle.auth.AuthenticatedSocketBuilder;
import com.blackberry.bdp.krackle.exceptions.AuthenticationException;
import java.util.HashMap;
import java.util.Properties;
import javax.security.auth.login.LoginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecurityConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(SecurityConfiguration.class);

	private final HashMap<String, Object> configuration;
	private final String kafkaServicePrincipal;
	private final AuthenticatedSocketBuilder.Protocol kafkaSecurityProtocol;
	private final String jaasLoginContextName;

	public SecurityConfiguration(Properties props)
		 throws AuthenticationException, LoginException {
		kafkaSecurityProtocol = AuthenticatedSocketBuilder.Protocol.valueOf(
			 props.getProperty("kafka.security.protocol", "PLAINTEXT").trim().toUpperCase());
		kafkaServicePrincipal = props.getProperty("kafka.security.protocol.service.principal", "kafka").trim();
		jaasLoginContextName = props.getProperty("kafka.client.jaas.login.context", "kafkaClient").trim();
		configuration = new HashMap<>();
		configureSecurity();
	}

	private void configureSecurity() throws AuthenticationException,
		 LoginException {
		switch (kafkaSecurityProtocol) {
			case PLAINTEXT:
				break;
			case SASL_PLAINTEXT:
				configureSecurity(jaasLoginContextName);
				break;
			default:
				throw new AuthenticationException(String.format(
					 "kafka.security.protocol=%s not recognized or supported",
					 kafkaSecurityProtocol));
		}
	}

	private void configureSecurity(String loginContextName)
		 throws AuthenticationException, LoginException {
		switch (kafkaSecurityProtocol) {
			case SASL_PLAINTEXT:
				Login login = new Login(loginContextName, new Login.ClientCallbackHandler());
				login.startThreadIfNeeded();
				configuration.put("subject", login.getSubject());
				configuration.put("clientPrincipal", login.getPrincipal());
				configuration.put("servicePrincipal", kafkaServicePrincipal);
				break;
			default:
				throw new AuthenticationException(String.format(
					 "kafka.security.protocol=%s not recognized or supported within a login context",
					 kafkaSecurityProtocol));
		}
	}

	/**
	 * @return the configuration
	 */
	public HashMap<String, Object> getConfiguration() {
		return configuration;
	}

	/**
	 * @return the kafkaSecurityProtocol
	 */
	public AuthenticatedSocketBuilder.Protocol getKafkaSecurityProtocol() {
		return kafkaSecurityProtocol;
	}

}
