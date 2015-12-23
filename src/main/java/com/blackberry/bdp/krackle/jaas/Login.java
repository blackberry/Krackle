/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

/**
 * This class was copied from the Apache ZooKeeper project:
 *
 * git@github.com:apache/zookeeper.git
 * 92707a6a84a7965df2d7d8ead0acb721b6e62878
 *
 * Adapted as required to work with Krackle and it's implementation
 * of AuthenticatedSocketBuilder where needed.  The following
 * major changes were performed:
 *
 * 1) Package name was changed
 * 2) Use of a login via ticket cache/kinit command disabled
 * 3) Use of org.apache.zookeeper.common.Time removed
 * 4) JavaDoc added where missing
 * 5) Required the principal to be found in the JAAS config
 * 6) Added getPrincipal()
 * 7) Added a client call back handler
 *
 */

/**
 * This class is responsible for refreshing Kerberos credentials for
 * logins for both Zookeeper client and server.
 * See ZooKeeperSaslServer for server-side usage.
 * See ZooKeeperSaslClient for client-side usage.
 */
import com.blackberry.bdp.krackle.Time;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.callback.CallbackHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.Subject;

import java.util.Date;
import java.util.Random;
import java.util.Set;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

public class Login {

	private static final Logger LOG = LoggerFactory.getLogger(Login.class);
	public CallbackHandler callbackHandler;

	// LoginThread will sleep until 80% of time from last refresh to
	// ticket's expiry has been reached, at which time it will wake
	// and try to renew the ticket.
	private static final float TICKET_RENEW_WINDOW = 0.80f;

	/**
	 * Percentage of random jitter added to the renewal time
	 */
	private static final float TICKET_RENEW_JITTER = 0.05f;

	// Regardless of TICKET_RENEW_WINDOW setting above and the ticket expiry time,
	// thread will not sleep between refresh attempts any less than 1 minute (60*1000 milliseconds = 1 minute).
	// Change the '1' to e.g. 5, to change this to 5 minutes.
	private static final long MIN_TIME_BEFORE_RELOGIN = 1 * 60 * 1000L;

	private Subject subject = null;
	private Thread t = null;
	private boolean isKrbTicket = false;
	private boolean isUsingTicketCache = false;

	/** Random number generator */
	private static final Random rng = new Random();

	private LoginContext login = null;
	private String loginContextName = null;
	private String principal = null;

	// Initialize 'lastLogin' to do a login at first time
	private long lastLogin = Time.currentElapsedTime() - MIN_TIME_BEFORE_RELOGIN;

	/**
	 * LoginThread constructor. The constructor starts the thread used
	 * to periodically re-login to the Kerberos Ticket Granting Server.
	 * @param loginContextName
	 *               name of section in JAAS file that will be use to login.
	 *               Passed as first param to javax.security.auth.login.LoginContext().
	 *
	 * @param callbackHandler
	 *               Passed as second param to javax.security.auth.login.LoginContext().
	 * @throws javax.security.auth.login.LoginException
	 *               Thrown if authentication fails.
	 */
	public Login(final String loginContextName, CallbackHandler callbackHandler)
		 throws LoginException {
		this.callbackHandler = callbackHandler;
		login = login(loginContextName);
		this.loginContextName = loginContextName;
		subject = login.getSubject();
		isKrbTicket = !subject.getPrivateCredentials(KerberosTicket.class).isEmpty();
		AppConfigurationEntry entries[] = Configuration.getConfiguration().getAppConfigurationEntry(loginContextName);
		for (AppConfigurationEntry entry : entries) {
			// there will only be a single entry, so this for() loop will only be iterated through once.
			if (entry.getOptions().get("useTicketCache") != null) {
				String val = (String) entry.getOptions().get("useTicketCache");
				if (val.equals("true")) {
					isUsingTicketCache = true;
				}
			}
			if (entry.getOptions().get("principal") != null) {
				principal = (String) entry.getOptions().get("principal");
			} else {
				throw new LoginException("could not determine principal from login context configuration");
			}
			break;
		}

		if (!isKrbTicket) {
			// if no TGT, do not bother with ticket management.
			return;
		}

		// Refresh the Ticket Granting Ticket (TGT) periodically. How often to refresh is determined by the
		// TGT's existing expiry date and the configured MIN_TIME_BEFORE_RELOGIN. For testing and development,
		// you can decrease the interval of expiration of tickets (for example, to 3 minutes) by running :
		//  "modprinc -maxlife 3mins <principal>" in kadmin.
		t = new Thread(new Runnable() {
			@Override
			public void run() {
				LOG.info("TGT refresh thread started.");
				while (true) {  // renewal thread's main loop. if it exits from here, thread will exit.
					KerberosTicket tgt = getTGT();
					long now = Time.currentWallTime();
					long nextRefresh;
					Date nextRefreshDate;
					if (tgt == null) {
						nextRefresh = now + MIN_TIME_BEFORE_RELOGIN;
						nextRefreshDate = new Date(nextRefresh);
						LOG.warn("No TGT found: will try again at {}", nextRefreshDate);
					} else {
						nextRefresh = getRefreshTime(tgt);
						long expiry = tgt.getEndTime().getTime();
						Date expiryDate = new Date(expiry);
						if ((isUsingTicketCache) && (tgt.getEndTime().equals(tgt.getRenewTill()))) {
							Object[] logPayload = {expiryDate, getPrincipal(), getPrincipal()};
							LOG.error("The TGT cannot be renewed beyond the next expiry date: {}."
								 + "This process will not be able to authenticate new SASL connections after that "
								 + "time (for example, it will not be authenticate a new connection with a Kafka "
								 + "broker).  Ask your system administrator to either increase the "
								 + "'renew until' time by doing : 'modprinc -maxrenewlife {}' within "
								 + "kadmin, or instead, to generate a keytab for {}. Because the TGT's "
								 + "expiry cannot be further extended by refreshing, exiting refresh thread now.", logPayload);
							return;
						}
						// determine how long to sleep from looking at ticket's expiry.
						// We should not allow the ticket to expire, but we should take into consideration
						// MIN_TIME_BEFORE_RELOGIN. Will not sleep less than MIN_TIME_BEFORE_RELOGIN, unless doing so
						// would cause ticket expiration.
						if ((nextRefresh > expiry)
							 || ((now + MIN_TIME_BEFORE_RELOGIN) > expiry)) {
							// expiry is before next scheduled refresh).
							nextRefresh = now;
						} else {
							if (nextRefresh < (now + MIN_TIME_BEFORE_RELOGIN)) {
								// next scheduled refresh is sooner than (now + MIN_TIME_BEFORE_LOGIN).
								Date until = new Date(nextRefresh);
								Date newuntil = new Date(now + MIN_TIME_BEFORE_RELOGIN);
								Object[] logPayload = {until, newuntil, (MIN_TIME_BEFORE_RELOGIN / 1000)};
								LOG.warn("TGT refresh thread time adjusted from : {} to : {} since "
									 + "the former is sooner than the minimum refresh interval ("
									 + "{} seconds) from now.", logPayload);
							}
							nextRefresh = Math.max(nextRefresh, now + MIN_TIME_BEFORE_RELOGIN);
						}
						nextRefreshDate = new Date(nextRefresh);
						if (nextRefresh > expiry) {
							Object[] logPayload = {nextRefreshDate, expiryDate};
							LOG.error("next refresh: {} is later than expiry {}."
								 + " This may indicate a clock skew problem. Check that this host and the KDC's "
								 + "hosts' clocks are in sync. Exiting refresh thread.", logPayload);
							return;
						}
					}
					if (now == nextRefresh) {
						LOG.info("refreshing now because expiry is before next scheduled refresh time.");
					} else {
						if (now < nextRefresh) {
							Date until = new Date(nextRefresh);
							LOG.info("TGT refresh sleeping until: {}", until.toString());
							try {
								Thread.sleep(nextRefresh - now);
							} catch (InterruptedException ie) {
								LOG.warn("TGT renewal thread has been interrupted and will exit.");
								break;
							}
						} else {
							LOG.error("nextRefresh:{} is in the past: exiting refresh thread. Check"
								 + " clock sync between this host and KDC - (KDC's clock is likely ahead of this host)."
								 + " Manual intervention will be required for this client to successfully authenticate."
								 + " Exiting refresh thread.", nextRefreshDate);
							break;
						}
					}
					if (isUsingTicketCache) {
						LOG.error("use of ticket cache and login via kinit on cmd line not supported");
						break;
					}
					try {
						int retry = 1;
						while (retry >= 0) {
							try {
								reLogin();
								break;
							} catch (LoginException le) {
								if (retry > 0) {
									--retry;
									// sleep for 10 seconds.
									try {
										Thread.sleep(10 * 1000);
									} catch (InterruptedException e) {
										LOG.error("Interrupted during login retry after LoginException:", le);
										throw le;
									}
								} else {
									LOG.error("Could not refresh TGT for principal: {}.", getPrincipal(), le);
								}
							}
						}
					} catch (LoginException le) {
						LOG.error("Failed to refresh TGT: refresh thread exiting now.", le);
						break;
					}
				}
			}

		});
		t.setDaemon(true);
	}

	public void startThreadIfNeeded() {
		// thread object 't' will be null if a refresh thread is not needed.
		if (t != null) {
			t.start();
		}
	}

	public void shutdown() {
		if ((t != null) && (t.isAlive())) {
			t.interrupt();
			try {
				t.join();
			} catch (InterruptedException e) {
				LOG.warn("error while waiting for Login thread to shutdown: ", e);
			}
		}
	}

	public Subject getSubject() {
		return subject;
	}

	public String getLoginContextName() {
		return loginContextName;
	}

	private synchronized LoginContext login(final String loginContextName) throws LoginException {
		if (loginContextName == null) {
			throw new LoginException("loginContext name null ");
		}
		LoginContext loginContext = new LoginContext(loginContextName, callbackHandler);
		loginContext.login();
		LOG.info("successfully logged in.");
		return loginContext;
	}

	// c.f. org.apache.hadoop.security.UserGroupInformation.
	private long getRefreshTime(KerberosTicket tgt) {
		long start = tgt.getStartTime().getTime();
		long expires = tgt.getEndTime().getTime();
		LOG.info("TGT valid starting at:        {}", tgt.getStartTime().toString());
		LOG.info("TGT expires:                  {}", tgt.getEndTime().toString());
		long proposedRefresh = start + (long) ((expires - start)
			 * (TICKET_RENEW_WINDOW + (TICKET_RENEW_JITTER * rng.nextDouble())));
		if (proposedRefresh > expires) {
			// proposedRefresh is too far in the future: it's after ticket expires: simply return now.
			return Time.currentWallTime();
		} else {
			return proposedRefresh;
		}
	}

	private synchronized KerberosTicket getTGT() {
		Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
		for (KerberosTicket ticket : tickets) {
			KerberosPrincipal server = ticket.getServer();
			if (server.getName().equals("krbtgt/" + server.getRealm() + "@" + server.getRealm())) {
				LOG.debug("Found tgt {}.", ticket);
				return ticket;
			}
		}
		return null;
	}

	private boolean hasSufficientTimeElapsed() {
		long now = Time.currentElapsedTime();
		if (now - getLastLogin() < MIN_TIME_BEFORE_RELOGIN) {
			LOG.warn("Not attempting to re-login since the last re-login was "
				 + "attempted less than {} seconds before.",
				 (MIN_TIME_BEFORE_RELOGIN / 1000));
			return false;
		}
		// register most recent relogin attempt
		setLastLogin(now);
		return true;
	}

	/**
	 * Returns login object
	 * @return login
	 */
	private LoginContext getLogin() {
		return login;
	}

	/**
	 * Set the login object
	 * @param login
	 */
	private void setLogin(LoginContext login) {
		this.login = login;
	}

	/**
	 * Set the last login time.
	 * @param time the number of milliseconds since the beginning of time
	 */
	private void setLastLogin(long time) {
		lastLogin = time;
	}

	/**
	 * Get the time of the last login.
	 * @return the number of milliseconds since the beginning of time.
	 */
	private long getLastLogin() {
		return lastLogin;
	}

	/**
	 * Re-login a principal. This method assumes that {@link #login(String)} has happened already.
	 * @throws javax.security.auth.login.LoginException on a failure
	 */
	// c.f. HADOOP-6559
	private synchronized void reLogin()
		 throws LoginException {
		if (!isKrbTicket) {
			return;
		}
		LoginContext existingLogin = getLogin();
		if (existingLogin == null) {
			throw new LoginException("login must be done first");
		}
		if (!hasSufficientTimeElapsed()) {
			return;
		}
		LOG.info("Initiating logout for {}", principal);
		synchronized (Login.class) {
			//clear up the kerberos state. But the tokens are not cleared! As per
			//the Java kerberos login module code, only the kerberos credentials
			//are cleared
			existingLogin.logout();
			//login and also update the subject field of this instance to
			//have the new credentials (pass it to the LoginContext constructor)
			existingLogin = new LoginContext(loginContextName, getSubject());
			LOG.info("Initiating re-login for {}", principal);
			existingLogin.login();
			setLogin(existingLogin);
		}
	}

	/**
	 * @return the principal
	 */
	public String getPrincipal() {
		return principal;
	}

	// The CallbackHandler interface here refers to
	// javax.security.auth.callback.CallbackHandler.
	// It should not be confused with Krackle packet callbacks like
	//  ClientCallbackHandler in SaslPlainTextAuthenticator
	public static class ClientCallbackHandler implements CallbackHandler {

		@Override
		public void handle(Callback[] callbacks) throws
			 UnsupportedCallbackException {
			for (Callback callback : callbacks) {
				if (callback instanceof NameCallback) {
					NameCallback nc = (NameCallback) callback;
					nc.setName(nc.getDefaultName());
				} else {
					if (callback instanceof PasswordCallback) {
						LOG.warn("Could not login: the client is being asked for a password");
					} else {
						if (callback instanceof RealmCallback) {
							RealmCallback rc = (RealmCallback) callback;
							rc.setText(rc.getDefaultText());
						} else {
							if (callback instanceof AuthorizeCallback) {
								AuthorizeCallback ac = (AuthorizeCallback) callback;
								String authid = ac.getAuthenticationID();
								String authzid = ac.getAuthorizationID();
								if (authid.equals(authzid)) {
									ac.setAuthorized(true);
								} else {
									ac.setAuthorized(false);
								}
								if (ac.isAuthorized()) {
									ac.setAuthorizedID(authzid);
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

}
