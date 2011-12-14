/***************************************************************************
 *                                                                         *
 *                             ConnectionPool.java                         *
 *                            -------------------                          *
 *   date                 : 13.06.2008, 15:34:07                           *
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

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import de.uniba.wiai.lspi.chord.com.CommunicationException;
import de.uniba.wiai.lspi.chord.data.ID;
import de.uniba.wiai.lspi.chord.data.URL;

/**
 * This class pools connections to remote peers, so that they can be reused by
 * several SocketProxies, that represent the same remote peer locally.
 * 
 * Connections are destroyed when no SocketProxy that uses them exists.
 * 
 * @author sven
 * 
 */
final class ConnectionPool {

	/**
	 * Singleton instance of this peer. Eager instantiation, as this is required
	 * anyway.
	 */
	private static final ConnectionPool instance = new ConnectionPool();

	/**
	 * The connections currently active.
	 */
	private HashMap<String, Connection> connections;

	/**
	 * Counter to create unique ids for proxies.
	 */
	private AtomicLong idCounter = new AtomicLong(-1L);

	/**
	 * Private constructor to prevent instantiation by others.
	 * 
	 */
	private ConnectionPool() {
		this.connections = new HashMap<String, Connection>();
	}

	/**
	 * Create an id for a SocketProxy. The id is immediately set on the proxy
	 * with help of {@link SocketProxy#setProxyID(String)}
	 * 
	 * @param p
	 *            The proxy to create the id for.
	 */
	void createProxyID(SocketProxy p) {
		p.setProxyID(ConnectionPool.createConnectionKey(p)
				+ idCounter.addAndGet(1L));
	}

	/**
	 * Returns the connection that can be used by the provided SocketProxy. If
	 * no connection exists it is created. If a connection exists than the
	 * existing connection is returned.
	 * 
	 * TODO: what if the connection has been disconnected because of a failure?
	 * In this case the connection is still referenced, but a new proxy will
	 * also receive an exception despite in the meantime the peer may have
	 * recovered! Possible solutions: Provide a method to reinitialize
	 * connection Discard old connection and create a new one.
	 * 
	 * @param p
	 *            A SocketProxy. Must not be null!
	 * @return The connection used by the SocketProxy p.
	 * @throws CommunicationException
	 */
	Connection getConnectionFor(SocketProxy p) throws CommunicationException {
		if (p == null) {
			throw new IllegalArgumentException();
		}
		synchronized (this.connections) {
			String key = ConnectionPool.createConnectionKey(p);
			Connection conn = this.connections.get(key);
			if (conn == null) {
				conn = new Connection(p);
				this.connections.put(key, conn);
			} else {
				conn.newClient(p);
			}
			return conn;
		}
	}

	/**
	 * Informs the ConnectionPool that p does not need its connection anymore.
	 * This method is responsible to destroy connections that are no longer
	 * required.
	 * 
	 * Should only be called by the SocketProxy itself!
	 * 
	 * @param p
	 *            The SocketProxy that does not need its connection anymore.
	 *            Must not be null!
	 */
	void releaseConnection(SocketProxy p) {
		if (p == null) {
			throw new IllegalArgumentException();
		}
		synchronized (this.connections) {
			String key = ConnectionPool.createConnectionKey(p);
			Connection conn = this.connections.get(key);
			if (conn.releaseConnection(p)) {
				this.connections.remove(key);
				conn.disconnect();
			}
		}
	}

	/**
	 * Method that creates a unique key for a Connection to be stored in
	 * {@link #proxies}.
	 * 
	 * This is important for the methods {@link #create(URL, URL)},
	 * {@link #create(URL, URL, ID)}, and {@link #disconnect()}, so that socket
	 * communication also works when it is used within one JVM.
	 * 
	 * 
	 * @param p
	 *            SocketProxy to create a connection key for. Must not be null.
	 * @return The key to store the SocketProxy
	 */
	private static String createConnectionKey(SocketProxy p) {
		return p.getUrlOfLocalNode().toString() + "->"
				+ p.getNodeURL().toString();
	}

	/**
	 * Shutdown all outgoing connections. Pending requests are terminated.
	 * 
	 */
	void shutDownAll() {
		synchronized (this.connections) {
			java.util.List<Connection> connectionList = new java.util.LinkedList<Connection>(
					this.connections.values());
			for (Connection conn : connectionList) {
				conn.disconnect();
			}
		}
	}

	/**
	 * Get the singleton instance of this ConnectionPool.
	 * 
	 * @return
	 */
	static final ConnectionPool getInstance() {
		return instance;
	}

}
