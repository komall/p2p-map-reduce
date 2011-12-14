/***************************************************************************
 *                                                                         *
 *                             Connection.java                             *
 *                            -------------------                          *
 *   date                 : 13.06.2008, 15:35:14                           *
 *   copyright            : (C) 2008 Distributed and                       *
 *                              Mobile Systems Group                       *
 *                              Lehrstuhl fuer Praktische Informatik       *
 *                              Universitaet Bamberg                       *
 *                              http://www.uni-bamberg.de/pi/              *
 *   email                : sven.kaffille@uni-bamberg.de                   *
 *                                                                         *
 *                                                                         *
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 *   A copy of the license can be found in the license.txt file supplied   *
 *   with this software or at: http://www.gnu.org/copyleft/gpl.html        *
 *                                                                         *
 ***************************************************************************/
package de.uniba.wiai.lspi.chord.com.socket;

import static de.uniba.wiai.lspi.util.logging.Logger.LogLevel.DEBUG;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.uniba.wiai.lspi.chord.com.CommunicationException;
import de.uniba.wiai.lspi.chord.com.Node;
import de.uniba.wiai.lspi.chord.data.URL;
import de.uniba.wiai.lspi.util.logging.Logger;

class Connection implements Runnable {

	/**
	 * List of ids (String) of Proxies using this connection. When this list is
	 * empty the connection can be closed. This is the responsibility of the
	 * ConnectionPool for synchronization reasons.
	 */
	private List<String> connectedProxies = Collections
			.synchronizedList(new LinkedList<String>());

	private boolean disconnected = false;

	/**
	 * The logger for this connection.
	 */
	private Logger logger;

	/**
	 * The socket used by this connection.
	 */
	private Socket mySocket;

	/**
	 * The InputStream associated with the socket of this connection.
	 */
	private ObjectInputStream in;

	/**
	 * The OutputStream associated with the socket of this connection.
	 */
	private ObjectOutputStream out;

	/**
	 * Counter to create unique request identifiers, so that waiting threads can
	 * be identified.
	 */
	private long requestCounter = 0;

	/**
	 * The URL of the peer this connection provides access to.
	 */
	private URL nodeURL;

	/**
	 * The URL of the local peer.
	 */
	private URL urlOflocalNode;

	/**
	 * Collection of responses received for requests.
	 */
	private Map<String, Response> responses;

	/**
	 * {@link Map} where threads are put in that are waiting for a repsonse.
	 * Key: identifier of the request (same as for the response). Value: The
	 * Thread itself.
	 */
	private Map<String, WaitingThread> waitingThreads;

	/**
	 * Create a new Connection.
	 * 
	 * @param p
	 *            The first SocketProxy for which this connection is created.
	 *            Must not be null!
	 * @throws CommunicationException
	 */
	Connection(SocketProxy p) throws CommunicationException {
		if (p == null) {
			throw new IllegalArgumentException();
		}
		this.logger = Logger.getLogger(this.getClass());
		this.nodeURL = p.getNodeURL();
		this.urlOflocalNode = p.getUrlOfLocalNode();
		this.newClient(p);
		this.makeSocketAvailable();
		logger.info("Connection " + this.urlOflocalNode + " -> " + this.nodeURL
				+ " initialized!");
	}

	/**
	 * This method establishes the connection if not already done.
	 * 
	 * @throws CommunicationException
	 */
	private void makeSocketAvailable() throws CommunicationException {
		 if (this.disconnected) {
			throw new CommunicationException("Connection from "
					+ this.urlOflocalNode + " to remote host " + this.nodeURL
					+ " is broken down. ");
		}

		logger.debug("makeSocketAvailable() called. "
				+ "Testing for socket availability");

		if (this.responses == null) {
			this.responses = new HashMap<String, Response>();
		}
		if (this.waitingThreads == null) {
			this.waitingThreads = new HashMap<String, WaitingThread>();
		}
		if (this.mySocket == null) {
			try {
				logger.info("Opening new socket to " + this.nodeURL);
				this.mySocket = new Socket(this.nodeURL.getHost(), this.nodeURL
						.getPort());
				logger.debug("Socket created: " + this.mySocket);
				this.mySocket.setSoTimeout(5000);
				this.out = new ObjectOutputStream(this.mySocket
						.getOutputStream());
				this.in = new ObjectInputStream(this.mySocket.getInputStream());
				logger.debug("Sending connection request!");
				out.writeObject(new Request(MethodConstants.CONNECT,
						"Initial Connection"));
				try {
					// set time out, in case the other side does not answer!
					Response resp = null;
					boolean timedOut = false;
					try {
						logger.debug("Waiting for connection response!");
						resp = (Response) in.readObject();
					} catch (SocketTimeoutException e) {
						logger.info("Connection timed out!");
						timedOut = true;
					}
					this.mySocket.setSoTimeout(0);
					if (timedOut) {
						throw new CommunicationException(
								"Connection to remote host timed out!");
					}
					if (resp != null
							&& resp.getStatus() == Response.REQUEST_SUCCESSFUL) {
						Thread t = new Thread(this, "Connection_Thread_"
								+ this.nodeURL);
						t.start();
					} else {
						throw new CommunicationException(
								"Establishing connection failed!");
					}
				} catch (ClassNotFoundException e) {
					throw new CommunicationException(
							"Unexpected result received! " + e.getMessage(), e);
				} catch (ClassCastException e) {
					throw new CommunicationException(
							"Unexpected result received! " + e.getMessage(), e);
				}
			} catch (UnknownHostException e) {
				throw new CommunicationException("Unknown host: "
						+ this.nodeURL.getHost());
			} catch (IOException ioe) {
				throw new CommunicationException(this.urlOflocalNode + ": Could not set up IO channel "
						+ "to host " + this.nodeURL.getHost() + " on port " + this.nodeURL.getPort() , ioe);
			}
		}
		logger.debug("makeSocketAvailable() finished. Socket " + this.mySocket);
	}

	void newClient(SocketProxy p) {
		if (!this.nodeURL.equals(p.getNodeURL())) {
			throw new IllegalArgumentException();
		}
		if (!this.urlOflocalNode.equals(p.getUrlOfLocalNode())) {
			throw new IllegalArgumentException();
		}
		this.connectedProxies.add(p.getProxyID());
		logger.debug("Added proxy " + p.getProxyID());
	}

	/**
	 * Must only be called by ConnectionPool!
	 * 
	 * @param p
	 */
	boolean releaseConnection(SocketProxy p) {
		if (!this.nodeURL.equals(p.getNodeURL())) {
			throw new IllegalArgumentException();
		}
		if (!this.urlOflocalNode.equals(p.getUrlOfLocalNode())) {
			throw new IllegalArgumentException();
		}
		this.connectedProxies.remove(p.getProxyID());
		logger.debug("Removed proxy " + p.getProxyID());
		return (this.connectedProxies.size() == 0);
	}

	/**
	 * Send a request to the remote peer.
	 * 
	 * @param p
	 *            The SocketProxy sending the Request. Must have been registered
	 *            before with {@link #newClient(SocketProxy)}.
	 * @param methodIdentifier
	 *            The method to invoke on the remote peer. See
	 *            {@link MethodConstants}.
	 * @param parameters
	 *            The parameters of the method.
	 * @return The created Request, that has been send to the remote peer.
	 * @throws CommunicationException
	 */
	Request send(SocketProxy p, int methodIdentifier, Serializable[] parameters)
			throws CommunicationException {
		logger.debug("Proxy " + p.getProxyID() + " tries to send. ProxyList"
				+ this.connectedProxies);
		if (p == null || !this.connectedProxies.contains(p.getProxyID())) {
			throw new IllegalArgumentException();
		}
		if (this.disconnected == true) {
			throw new CommunicationException("Connection has been lost!");
		}
		Request request = this.createRequest(methodIdentifier, parameters);
		try {
			logger.debug("Sending request " + request.getReplyWith());
			synchronized (this.out) {
				this.out.writeObject(request);
				this.out.flush();
				this.out.reset();
			}
			return request;
		} catch (IOException e) {
			throw new CommunicationException("Could not connect to node! "
					+ e.getMessage(), e);
		}
	}

	/**
	 * Creates a request for the method identified by
	 * <code>methodIdentifier</code> with the parameters
	 * <code>parameters</code>. Sets also field
	 * {@link Request#getReplyWith()}of created {@link Request request}.
	 * 
	 * @param methodIdentifier
	 *            The identifier of the method to request.
	 * @param parameters
	 *            The parameters for the request.
	 * @return The {@link Request request}created.
	 */
	private Request createRequest(int methodIdentifier,
			Serializable[] parameters) {
		if (logger.isEnabledFor(DEBUG)) {
			logger.debug("Creating request for method "
					+ MethodConstants.getMethodName(methodIdentifier)
					+ " with parameters "
					+ java.util.Arrays.deepToString(parameters));
		}
		String responseIdentifier = this.createIdentifier(methodIdentifier);
		Request request = new Request(methodIdentifier, responseIdentifier);
		request.setParameters(parameters);
		logger.debug("Request " + request + " created.");
		return request;
	}

	/**
	 * Private method to create an identifier that enables this to associate a
	 * {@link Response response}with a {@link Request request}made before.
	 * This method is synchronized to protect {@link #requestCounter}from race
	 * conditions.
	 * 
	 * @param methodIdentifier
	 *            Integer identifying the method this method is called from.
	 * @return Unique Identifier for the request.
	 */
	private synchronized String createIdentifier(int methodIdentifier) {
		/* Create unique identifier from */
		StringBuilder uid = new StringBuilder();
		/* Time stamp */
		uid.append(System.currentTimeMillis());
		uid.append("-");
		/* counter and */
		uid.append(this.requestCounter++);
		/* methodIdentifier */
		uid.append("-");
		uid.append(methodIdentifier);
		return uid.toString();
	}

	/**
	 * This method has to be called by a thread that sent a Request before in
	 * order to receive the response. The thread is blocked until a response is
	 * received or the connection to the remote peer has been destroyed.
	 * 
	 * @param request
	 *            The Request for which the thread awaits the Response. This
	 *            must be the return-value of a previous invocation of
	 *            {@link #send(SocketProxy, int, Serializable[])}.
	 * @return The Response to the provided Request.
	 * @throws CommunicationException
	 */
	Response waitForResponse(Request request) throws CommunicationException {
		if (request == null) {
			throw new IllegalArgumentException();
		}
		String responseIdentifier = request.getReplyWith();
		Response response = null;
		logger.debug("Trying to wait for response with identifier "
				+ responseIdentifier + " for method "
				+ MethodConstants.getMethodName(request.getRequestType()));

		synchronized (this.responses) {
			logger.debug("No of responses " + this.responses.size());
			/* Test if we got disconnected while waiting for lock on object */
			if (this.disconnected) {
				throw new CommunicationException("Connection to remote host "
						+ " is broken down. ");
			}
			/*
			 * Test if response is already available (Maybe response arrived
			 * before we reached this point).
			 */
			response = this.responses.remove(responseIdentifier);
			if (response != null) {
				return response;
			}

			/* WAIT FOR RESPONSE */
			/* add current thread to map of threads waiting for a response */
			WaitingThread wt = new WaitingThread(Thread.currentThread());
			this.waitingThreads.put(responseIdentifier, wt);
			while (!wt.hasBeenWokenUp()) {
				try {
					/*
					 * Wait until notified or time out is reached.
					 */
					logger.debug("Waiting for response to arrive.");
					this.responses.wait();
				} catch (InterruptedException e) {
					/*
					 * does not matter as this is intended Thread is interrupted
					 * if response arrives
					 */
				}
			}
			logger.debug("Have been woken up from waiting for response.");

			/* remove thread from map of threads waiting for a response */
			this.waitingThreads.remove(responseIdentifier);
			/* try to get the response if available */
			response = this.responses.remove(responseIdentifier);
			logger.debug("Response for request with identifier "
					+ responseIdentifier + " for method "
					+ MethodConstants.getMethodName(request.getRequestType())
					+ " received.");
			/* if no response availabe */
			if (response == null) {
				logger.debug("No response received.");
				/* we have been disconnected */
				if (this.disconnected) {
					logger.info("Connection to remote host lost.");
					throw new CommunicationException(
							"Connection to remote host " + " is broken down. ");
				}
				/* or time out has elapsed */
				else {
					logger.error("There is no result, but we have not been "
							+ "disconnected. Something went seriously wrong!");
					throw new CommunicationException(
							"Did not receive a response!");
				}
			}
		}
		return response;
	}

	/**
	 * The run methods waits for incoming {@link Response}s and puts them into
	 * a data structure from where they can be collected by the associated
	 * method call that sent a {@link Request} to the remote peer to that this
	 * connection provides access.
	 */
	public void run() {
		while (!this.disconnected) {
			try {
				Response response = (Response) this.in.readObject();
				logger.debug("Response " + response + "received!");
				if (response.getMethodIdentifier() == MethodConstants.SHUTDOWN) {
					// the other side is shutting down
					this.disconnected = true;
					this.connectionClosed();
					try {
						this.mySocket.close();
					} catch (IOException e) {
						this.logger.error("Exception while closing Socket", e);
					}
					this.mySocket = null;
					this.responses = null;
					this.waitingThreads = null;
				} else {
					this.responseReceived(response);
				}
			} catch (ClassNotFoundException cnfe) {
				/* should not occur, as all classes must be locally available */
				logger
						.fatal(
								"ClassNotFoundException occured during deserialization "
										+ "of response. There is something seriously wrong "
										+ " here! ", cnfe);
			} catch (IOException e) {
				if (!this.disconnected) {
					logger.warn("Could not read response from stream!", e);
				} else {
					logger.debug(this + ": Connection has been closed!");
				}
				this.connectionClosed();
			}
		}
	}

	/**
	 * This method is called by {@link #run()}when it receives a
	 * {@link Response}. The {@link Thread thread} waiting for the response is
	 * woken up and the response is put into {@link Map responses}.
	 * 
	 * @param response
	 */
	private void responseReceived(Response response) {
		synchronized (this.responses) {
			/* Try to fetch thread waiting for this response */
			logger.debug("No of waiting threads " + this.waitingThreads);
			WaitingThread waitingThread = this.waitingThreads.get(response
					.getInReplyTo());
			logger.debug("Response with id " + response.getInReplyTo()
					+ "received.");
			/* save response */
			this.responses.put(response.getInReplyTo(), response);
			/* if there is a thread waiting for this response */
			if (waitingThread != null) {
				/* wake up the thread */
				logger.debug("Waking up thread!");
				waitingThread.wakeUp();
			} else {
				// TODO what else? why 'else' anyway?
			}
		}
	}

	/**
	 * Tells this connection that it is not needed anymore. Must only be invoked
	 * by ConnectionPool!
	 */
	void disconnect() {

		logger.info("Destroying connection");

		this.disconnected = true;
		try {
			if (this.out != null) {
				try {
					/*
					 * notify endpoint this is connected to, about shut down of
					 * this connection
					 */
					logger.debug("Sending shutdown notification to endpoint.");
					Request request = this.createRequest(
							MethodConstants.SHUTDOWN, new Serializable[0]);
					logger.debug("Notification send.");
					this.out.writeObject(request);
					this.out.close();
					this.out = null;
					logger.debug("OutputStream " + this.out + " closed.");
				} catch (IOException e) {
					/* should not occur */
					logger.debug(this
							+ ": Exception during closing of output stream "
							+ this.out, e);
				}
			}
			if (this.in != null) {
				try {
					this.in.close();
					logger.debug("InputStream " + this.in + " closed.");
					this.in = null;
				} catch (IOException e) {
					/* should not occur */
					logger.debug("Exception during closing of input stream"
							+ this.in);
				}
			}
			if (this.mySocket != null) {
				try {
					logger.info("Closing socket " + this.mySocket + ".");
					this.mySocket.close();
				} catch (IOException e) {
					/* should not occur */
					logger.debug("Exception during closing of socket "
							+ this.mySocket);
				}
				this.mySocket = null;
			}
		} catch (Throwable t) {
			logger.warn(
					"Unexpected exception during disconnection of SocketProxy",
					t);
		}
		this.connectionClosed();
	}

	/**
	 * Method to indicate that connection to remote {@link Node node} has been
	 * closed. Necessary to wake up threads possibly waiting for a response.  
	 */
	private void connectionClosed() {
		if (this.responses == null) {
			/*
			 * Nothing to do!
			 */
			return;
		}
		/* synchronize on responses, as all threads accessing this proxy do so */
		synchronized (this.responses) {
			logger.info("Connection broken down!");
			this.disconnected = true;
			/* wake up all threads */
			for (WaitingThread thread : this.waitingThreads.values()) {
				logger.debug("Interrupting waiting thread " + thread);
				thread.wakeUp();
			}
		}
	}

	/**
	 * Wraps a thread, which is waiting for a response.
	 * 
	 * @author sven
	 * 
	 */
	private static class WaitingThread {

		private boolean hasBeenWokenUp = false;

		private Thread thread;

		private WaitingThread(Thread thread) {
			this.thread = thread;
		}

		/**
		 * Returns <code>true</code> when the thread has been woken up by
		 * invoking {@link #wakeUp()}
		 * 
		 * @return
		 */
		boolean hasBeenWokenUp() {
			return this.hasBeenWokenUp;
		}

		/**
		 * Wake up the thread that is waiting for a response.
		 * 
		 */
		void wakeUp() {
			this.hasBeenWokenUp = true;
			this.thread.interrupt();
		}

		public String toString() {
			return this.thread.toString() + ": Waiting? "
					+ !this.hasBeenWokenUp();
		}
	}
}
