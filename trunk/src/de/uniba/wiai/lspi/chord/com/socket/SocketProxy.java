/***************************************************************************
 *                                                                         *
 *                             SocketProxy.java                            *
 *                            -------------------                          *
 *   date                 : 12.08.2004                                     *
 *   copyright            : (C) 2004-2008 Distributed and                  *
 *                              Mobile Systems Group                       *
 *                              Lehrstuhl fuer Praktische Informatik       *
 *                              Universitaet Bamberg                       *
 *                              http://www.uni-bamberg.de/pi/              *
 *   email                : sven.kaffille@uni-bamberg.de                   *
 *                          karsten.loesing@uni-bamberg.de                 *
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

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import de.uniba.wiai.lspi.chord.com.CommunicationException;
import de.uniba.wiai.lspi.chord.com.Endpoint;
import de.uniba.wiai.lspi.chord.com.Node;
import de.uniba.wiai.lspi.chord.com.Proxy;
import de.uniba.wiai.lspi.chord.com.RefsAndEntries;
import de.uniba.wiai.lspi.chord.data.Entry;
import de.uniba.wiai.lspi.chord.data.ID;
import de.uniba.wiai.lspi.chord.data.URL;
import de.uniba.wiai.lspi.util.logging.Logger;

/**
 * This is the implementation of {@link Proxy} for the socket protocol. This
 * connects to the {@link SocketEndpoint endpoint} of the node it represents by
 * means of <code>Sockets</code>.
 * 
 * TODO: Use SocketTimeout to determine if Proxy is still needed. Therefore
 * remove HashMap that contains all SocketProxies. Make Thread that executes
 * this proxy a daemon thread.
 * 
 * @author sven
 * @version 1.0.5
 */
public final class SocketProxy extends Proxy {

	/**
	 * The logger for instances of this class.
	 */
	private final static Logger logger = Logger.getLogger(SocketProxy.class);

	/**
	 * The connection pool that manages the outgoing connections for the local
	 * peer.
	 */
	private static final ConnectionPool connPool = ConnectionPool.getInstance();

	/**
	 * The {@link URL}of the node that uses this proxy to connect to the node,
	 * which is represented by this proxy.
	 * 
	 */
	private URL urlOfLocalNode = null;

	/**
	 * The socket that provides the connection to the node that this is the
	 * Proxy for. This is transient as a proxy can be transferred over the
	 * network. After transfer this socket has to be restored by reconnecting to
	 * the node.
	 */
	private transient Connection connection;

	/**
	 * Locally unique id of this proxy.
	 */
	private String proxyID = "";

	/**
	 * Establishes a connection from <code>urlOfLocalNode</code> to
	 * <code>url</code>. The connection is represented by the returned
	 * <code>SocketProxy</code>.
	 * 
	 * @param url
	 *            The {@link URL} to connect to.
	 * @param urlOfLocalNode
	 *            {@link URL} of local node that establishes the connection.
	 * @return <code>SocketProxy</code> representing the established
	 *         connection.
	 * @throws CommunicationException
	 *             Thrown if establishment of connection to <code>url</code>
	 *             failed.
	 */
	public static SocketProxy create(URL urlOfLocalNode, URL url)
			throws CommunicationException {
		SocketProxy newProxy = new SocketProxy(url, urlOfLocalNode);
		return newProxy;
	}

	/**
	 * Closes all outgoing connections to other peers. Allows the local peer to
	 * shutdown cleanly.
	 * 
	 */
	static void shutDownAll() {
		connPool.shutDownAll();
	}

	/**
	 * Creates a <code>SocketProxy</code> representing the connection from
	 * <code>urlOfLocalNode</code> to <code>url</code>. The connection is
	 * established when the first (remote) invocation with help of the
	 * <code>SocketProxy</code> occurs.
	 * 
	 * @param url
	 *            The {@link URL} of the remote node.
	 * @param urlOfLocalNode
	 *            The {@link URL} of local node.
	 * @param nodeID
	 *            The {@link ID} of the remote node.
	 * @return SocketProxy
	 */
	protected static SocketProxy create(URL url, URL urlOfLocalNode, ID nodeID) {
		SocketProxy proxy = new SocketProxy(url, urlOfLocalNode, nodeID);
		return proxy;
	}

	/**
	 * Corresponding constructor to factory method {@link #create(URL, URL, ID)}.
	 * 
	 * @see #create(URL, URL, ID)
	 * @param url
	 * @param urlOfLocalNode1
	 * @param nodeID1
	 */
	protected SocketProxy(URL url, URL urlOfLocalNode1, ID nodeID1) {
		super(url);
		if (url == null || urlOfLocalNode1 == null || nodeID1 == null) {
			throw new IllegalArgumentException("null");
		}
		this.urlOfLocalNode = urlOfLocalNode1;
		connPool.createProxyID(this);
		this.nodeID = nodeID1;
	}

	/**
	 * Corresponding constructor to factory method {@link #create(URL, URL)}.
	 * 
	 * @see #create(URL, URL)
	 * @param url
	 * @param urlOfLocalNode1
	 * @throws CommunicationException
	 */
	private SocketProxy(URL url, URL urlOfLocalNode1)
			throws CommunicationException {
		super(url);
		if (url == null || urlOfLocalNode1 == null) {
			throw new IllegalArgumentException("URLs must not be null!");
		}
		this.urlOfLocalNode = urlOfLocalNode1;
		connPool.createProxyID(this);
		this.initializeNodeID();
		logger.info("SocketProxy for " + url + " has been created.");
	}

	/**
	 * @param key
	 * @return The successor of <code>key</code>.
	 * @throws CommunicationException
	 */
	public Node findSuccessor(ID key) throws CommunicationException {
		this.makeConnectionAvailable();

		/* send request */
		Request request = null;
		try {
			logger.debug("Trying to find successor for ID " + key);
			request = this.connection.send(this,
					MethodConstants.FIND_SUCCESSOR, new Serializable[] { key });
		} catch (CommunicationException ce) {
			logger.debug("Connection failed!");
			throw ce;
		}
		/* wait for response */
		logger.debug("Waiting for response for request " + request);
		Response response = this.connection.waitForResponse(request);
		logger.debug("Response " + response + " arrived.");
		if (response.isFailureResponse()) {
			throw new CommunicationException(response.getFailureReason());
		} else {
			try {
				RemoteNodeInfo nodeInfo = (RemoteNodeInfo) response.getResult();
				if (nodeInfo.getNodeURL().equals(this.urlOfLocalNode)) {
					return Endpoint.getEndpoint(this.urlOfLocalNode).getNode();
				} else {
					return create(nodeInfo.getNodeURL(), this.urlOfLocalNode,
							nodeInfo.getNodeID());
				}
			} catch (ClassCastException e) {
				/*
				 * This should not occur as all nodes should have the same
				 * classes!
				 */
				String message = "Could not understand result! "
						+ response.getResult();
				logger.fatal(message);
				throw new CommunicationException(message, e);
			}
		}
	}

	/**
	 * @return The id of the node represented by this proxy.
	 * @throws CommunicationException
	 */
	private void initializeNodeID() throws CommunicationException {
		if (this.nodeID == null) {
			this.makeConnectionAvailable();

			logger.debug("Trying to get node ID ");
			/* send request */
			Request request = null;
			try {
				request = this.connection.send(this,
						MethodConstants.GET_NODE_ID, new Serializable[0]);
			} catch (CommunicationException ce) {
				logger.debug("Connection failed!");
				throw ce;
			}
			/* wait for response */
			logger.debug("Waiting for response for request " + request);
			Response response = this.connection.waitForResponse(request);
			logger.debug("Response " + response + " arrived.");
			if (response.isFailureResponse()) {
				throw new CommunicationException(response.getFailureReason());
			} else {
				try {
					this.nodeID = (ID) response.getResult();
				} catch (ClassCastException e) {
					/*
					 * This should not occur as all nodes should have the same
					 * classes!
					 */
					String message = "Could not understand result! "
							+ response.getResult();
					logger.fatal(message);
					throw new CommunicationException(message);
				}
			}
		}
	}

	/**
	 * @param potentialPredecessor
	 * @return List of references for the node invoking this method. See
	 *         {@link Node#notify(Node)}.
	 */
	@SuppressWarnings("unchecked")
	public List<Node> notify(Node potentialPredecessor)
			throws CommunicationException {
		this.makeConnectionAvailable();

		RemoteNodeInfo nodeInfoToSend = new RemoteNodeInfo(potentialPredecessor
				.getNodeURL(), potentialPredecessor.getNodeID());

		/* send request */
		Request request = null;
		try {
			request = this.connection.send(this, MethodConstants.NOTIFY,
					new Serializable[] { nodeInfoToSend });
		} catch (CommunicationException ce) {
			logger.debug("Connection failed!");
			throw ce;
		}

		/* wait for response to arrive */
		Response response = this.connection.waitForResponse(request);
		if (response.isFailureResponse()) {
			throw new CommunicationException(response.getFailureReason());
		} else {
			try {
				List<RemoteNodeInfo> references = (List<RemoteNodeInfo>) response
						.getResult();
				List<Node> nodes = new LinkedList<Node>();
				for (RemoteNodeInfo nodeInfo : references) {
					if (nodeInfo.getNodeURL().equals(this.urlOfLocalNode)) {
						nodes.add(Endpoint.getEndpoint(this.urlOfLocalNode)
								.getNode());
					} else {
						nodes.add(create(nodeInfo.getNodeURL(),
								this.urlOfLocalNode, nodeInfo.getNodeID()));
					}
				}
				return nodes;
			} catch (ClassCastException cce) {
				throw new CommunicationException(
						"Could not understand result! " + response.getResult(),
						cce);
			}
		}
	}

	/**
	 * @throws CommunicationException
	 */
	public void ping() throws CommunicationException {
		this.makeConnectionAvailable();

		boolean debugEnabled = SocketProxy.logger.isEnabledFor(DEBUG);

		if (debugEnabled) {
			logger.debug("Trying to ping remote node " + this.nodeURL);
		}

		/* send request */
		Request request = null;
		try {
			request = this.connection.send(this, MethodConstants.PING,
					new Serializable[0]);
		} catch (CommunicationException ce) {
			logger.debug("Connection failed!");
			throw ce;
		}
		/* wait for response */
		if (debugEnabled) {
			logger.debug("Waiting for response for request " + request);
		}
		Response response = this.connection.waitForResponse(request);
		if (debugEnabled) {
			logger.debug("Response " + response + " arrived.");
		}
		if (response.isFailureResponse()) {
			throw new CommunicationException(response.getFailureReason());
		} else {
			return;
		}

	}

	/**
	 * @param entry
	 * @throws CommunicationException
	 */
	public void insertEntry(Entry entry) throws CommunicationException {
		this.makeConnectionAvailable();

		logger.debug("Trying to insert entry " + entry + ".");

		/* send request */
		Request request = null;
		try {
			request = this.connection.send(this, MethodConstants.INSERT_ENTRY,
					new Serializable[] { entry });
		} catch (CommunicationException ce) {
			logger.debug("Connection failed!");
			throw ce;
		}
		/* wait for response */
		logger.debug("Waiting for response for request " + request);
		Response response = this.connection.waitForResponse(request);
		logger.debug("Response " + response + " arrived.");
		if (response.isFailureResponse()) {
			throw new CommunicationException(response.getFailureReason());
		} else {
			/* No result here */
			return;
		}
	}

	/**
	 * @param replicas
	 * @throws CommunicationException
	 */
	public void insertReplicas(Set<Entry> replicas)
			throws CommunicationException {
		this.makeConnectionAvailable();

		logger.debug("Trying to insert replicas " + replicas + ".");

		/* send request */
		Request request = null;
		try {
			request = this.connection.send(this,
					MethodConstants.INSERT_REPLICAS,
					new Serializable[] { (Serializable) replicas });
		} catch (CommunicationException ce) {
			logger.debug("Connection failed!");
			throw ce;
		}
		/* wait for response */
		logger.debug("Waiting for response for request " + request);
		Response response = this.connection.waitForResponse(request);
		logger.debug("Response " + response + " arrived.");
		if (response.isFailureResponse()) {
			throw new CommunicationException(response.getFailureReason());
		} else {
			/* No result here */
			return;
		}
	}

	/**
	 * @param predecessor
	 * @throws CommunicationException
	 */
	public void leavesNetwork(Node predecessor) throws CommunicationException {
		this.makeConnectionAvailable();

		logger.debug("Trying to insert notify node that " + predecessor
				+ " leaves network.");

		RemoteNodeInfo nodeInfo = new RemoteNodeInfo(predecessor.getNodeURL(),
				predecessor.getNodeID());

		/* send request */
		Request request = null;
		try {
			request = this.connection.send(this,
					MethodConstants.LEAVES_NETWORK,
					new Serializable[] { nodeInfo });
		} catch (CommunicationException ce) {
			logger.debug("Connection failed!");
			throw ce;
		}
		/* wait for response */
		logger.debug("Waiting for response for request " + request);
		Response response = this.connection.waitForResponse(request);
		logger.debug("Response " + response + " arrived.");
		if (response.isFailureResponse()) {
			throw new CommunicationException(response.getFailureReason());
		} else {
			/* No result here */
			return;
		}
	}

	/**
	 * @param entry
	 * @throws CommunicationException
	 */
	public void removeEntry(Entry entry) throws CommunicationException {
		this.makeConnectionAvailable();

		logger.debug("Trying to remove entry " + entry + ".");

		/* send request */
		Request request = null;
		try {
			request = this.connection.send(this, MethodConstants.REMOVE_ENTRY,
					new Serializable[] { entry });
		} catch (CommunicationException ce) {
			logger.debug("Connection failed!");
			throw ce;
		}
		/* wait for response */
		logger.debug("Waiting for response for request " + request);
		Response response = this.connection.waitForResponse(request);
		logger.debug("Response " + response + " arrived.");
		if (response.isFailureResponse()) {
			throw new CommunicationException(response.getFailureReason());
		} else {
			/* No result here */
			return;
		}

	}

	/**
	 * @param sendingNodeID
	 * @param replicas
	 * @throws CommunicationException
	 */
	public void removeReplicas(ID sendingNodeID, Set<Entry> replicas)
			throws CommunicationException {
		this.makeConnectionAvailable();

		logger.debug("Trying to remove replicas " + replicas + ".");

		/* prepare request for method insertEntry */
		Request request = null;
		try {
			request = this.connection.send(this,
					MethodConstants.REMOVE_REPLICAS, new Serializable[] {
							sendingNodeID, (Serializable) replicas });
		} catch (CommunicationException ce) {
			logger.debug("Connection failed!");
			throw ce;
		}
		/* wait for response */
		logger.debug("Waiting for response for request " + request);
		Response response = this.connection.waitForResponse(request);
		logger.debug("Response " + response + " arrived.");
		if (response.isFailureResponse()) {
			throw new CommunicationException(response.getFailureReason());
		} else {
			/* No result here */
			return;
		}
	}

	@SuppressWarnings("unchecked")
	public Set<Entry> retrieveEntries(ID id) throws CommunicationException {
		this.makeConnectionAvailable();

		logger.debug("Trying to retrieve entries for ID " + id);

		/* prepare request for method findSuccessor */
		Request request = null;
		try {
			request = this.connection
					.send(this, MethodConstants.RETRIEVE_ENTRIES,
							new Serializable[] { id });
		} catch (CommunicationException ce) {
			logger.debug("Connection failed!");
			throw ce;
		}
		/* wait for response */
		logger.debug("Waiting for response for request " + request);
		Response response = this.connection.waitForResponse(request);
		logger.debug("Response " + response + " arrived.");
		if (response.isFailureResponse()) {
			throw new CommunicationException(response.getFailureReason(),
					response.getThrowable());
		} else {
			try {
				Set<Entry> result = (Set<Entry>) response.getResult();
				return result;
			} catch (ClassCastException cce) {
				throw new CommunicationException(
						"Could not understand result! " + response.getResult());
			}
		}
	}

	/**
	 * This method has to be called at first in every method that uses the
	 * socket to connect to the node this is the proxy for. This method
	 * establishes the connection if not already done. This method has to be
	 * called as this proxy can be serialized and the reference to the socket is
	 * transient. So by calling this method after a transfer the connection to
	 * the node is reestablished. The same applies for {@link #logger}and
	 * {@link #responses}.
	 * 
	 * @throws CommunicationException
	 */
	private void makeConnectionAvailable() throws CommunicationException {
		if (this.connection == null) {
			this.connection = connPool.getConnectionFor(this);
		}
	}

	/**
	 * Finalization ensures that the underlying connection is closed if this
	 * proxy is the last one that used it.
	 * 
	 * @throws Throwable
	 */
	protected void finalize() throws Throwable {
		logger.debug("Finalization running.");
		if (this.connection == null) {
			connPool.releaseConnection(this);
		}
	}

	/**
	 * @param potentialPredecessor
	 * @return See {@link Node#notifyAndCopyEntries(Node)}.
	 * @throws CommunicationException
	 */
	public RefsAndEntries notifyAndCopyEntries(Node potentialPredecessor)
			throws CommunicationException {
		this.makeConnectionAvailable();

		RemoteNodeInfo nodeInfoToSend = new RemoteNodeInfo(potentialPredecessor
				.getNodeURL(), potentialPredecessor.getNodeID());

		/* prepare request for method notifyAndCopyEntries */
		Request request = null;
		/* send request */
		try {
			logger.debug("Trying to send request " + request);
			request = this.connection.send(this,
					MethodConstants.NOTIFY_AND_COPY,
					new Serializable[] { nodeInfoToSend });
		} catch (CommunicationException ce) {
			logger.debug("Connection failed!");
			throw ce;
		}
		/* wait for response */
		logger.debug("Waiting for response for request " + request);
		Response response = this.connection.waitForResponse(request);
		logger.debug("Response " + response + " arrived.");
		if (response.isFailureResponse()) {
			throw new CommunicationException(response.getFailureReason(),
					response.getThrowable());
		} else {
			try {
				RemoteRefsAndEntries result = (RemoteRefsAndEntries) response
						.getResult();
				List<Node> newReferences = new LinkedList<Node>();
				List<RemoteNodeInfo> references = result.getNodeInfos();
				for (RemoteNodeInfo nodeInfo : references) {
					if (nodeInfo.getNodeURL().equals(this.urlOfLocalNode)) {
						newReferences.add(Endpoint.getEndpoint(
								this.urlOfLocalNode).getNode());
					} else {
						newReferences.add(create(nodeInfo.getNodeURL(),
								this.urlOfLocalNode, nodeInfo.getNodeID()));
					}
				}
				return new RefsAndEntries(newReferences, result.getEntries());
			} catch (ClassCastException cce) {
				throw new CommunicationException(
						"Could not understand result! " + response.getResult());
			}
		}
	}

	/**
	 * The string representation of this proxy. Created when {@link #toString()}
	 * is invoked for the first time.
	 */
	private String stringRepresentation = null;

	/**
	 * @return String representation of this.
	 */
	public String toString() {
		if (this.nodeID == null || this.connection == null) {
			return "Unconnected SocketProxy from " + this.urlOfLocalNode
					+ " to " + this.nodeURL;
		}
		if (this.stringRepresentation == null) {
			StringBuilder builder = new StringBuilder();
			builder.append("SocketProxy from Node[conn.=");
			builder.append(this.connection);
			builder.append("] to Node[id=");
			builder.append(this.nodeID);
			builder.append(", url=");
			builder.append(this.nodeURL);
			builder.append("]");
			this.stringRepresentation = builder.toString();
		}
		return this.stringRepresentation;
	}

	/**
	 * @return the urlOfLocalNode
	 */
	final URL getUrlOfLocalNode() {
		return urlOfLocalNode;
	}

	/**
	 * @return the proxyID
	 */
	final String getProxyID() {
		return proxyID;
	}

	void setProxyID(String string) {
		this.proxyID = string;
	}

}
