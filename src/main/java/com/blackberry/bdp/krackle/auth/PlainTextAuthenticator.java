/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.blackberry.bdp.krackle.auth;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;


public class PlainTextAuthenticator implements Authenticator {

	private final Socket socket;

	/**
	 * Will create a socket based on host name and port
	 * @param hostname
	 * @param port
	 * @throws IOException
	 */
	public PlainTextAuthenticator(String hostname, int port)
		 throws IOException {
		this(new Socket(hostname, port));
	}

	/**
	 * Will use an existing socket
	 * @param socket
	 * @throws IOException
	 */
	public PlainTextAuthenticator(Socket socket)
		 throws IOException {
		this.socket = socket;
	}

	@Override
	public void configure(Map<String, ?> configs)
		 throws InvalidConfigurationTypeException,
		 MissingConfigurationException,
		 Exception {

	}

	@Override
	public void authenticate() throws IOException {

	}

	@Override
	public boolean configured() {
		return true;
	}

	@Override
	public boolean complete() {
		return true;
	}

	@Override
	public Socket getSocket() {
		return socket;
	}

	@Override
	public void close() throws IOException {

	}

}
