/***************************************************************************
 *                                                                         *
 *                             ThreadProxy.java                            *
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
package de.uniba.wiai.lspi.chord.com.local;

import de.uniba.wiai.lspi.chord.com.CommunicationException;
import de.uniba.wiai.lspi.chord.com.Endpoint;
import de.uniba.wiai.lspi.chord.com.Node;
import de.uniba.wiai.lspi.chord.com.Proxy;
import de.uniba.wiai.lspi.chord.com.RefsAndEntries;
import de.uniba.wiai.lspi.chord.data.Entry;
import de.uniba.wiai.lspi.chord.data.ID;
import de.uniba.wiai.lspi.chord.data.URL;
import de.uniba.wiai.lspi.util.logging.Logger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * This class represents a {@link Proxy} for the protocol that allows to be
 * build a (local) chord network within one JVM.
 * 
 * @author sven
 * @version 1.0.6
 */
public final class ThreadProxy extends Proxy {

	/**
	 * The logger for instances of this.
	 */
	private static final Logger logger = Logger.getLogger(ThreadProxy.class
			.getName());

	/**
	 * Reference to the {@link Registry registry}singleton.
	 */
	protected Registry registry = null;

	/**
	 * The {@link URL}of the node that created this proxy.
	 */
	protected URL creatorURL;

	/**
	 * Indicates if this proxy can be used for communication;
	 */
	protected boolean isValid = true;

	/**
	 * Indicates if this proxy has been used to make a invocation.
	 */
	protected boolean hasBeenUsed = false;

	private final String endpointID;

	//private int creationMode = -1;

	/**
	 * Parameters must not be null!
	 * 
	 * @param creatorURL1
	 * @param url
	 * @param nodeID1
	 * @param endpointID
	 */
	protected ThreadProxy(URL creatorURL1, URL url, ID nodeID1,
			String endpointID1) throws CommunicationException {
		super(url);
		if (creatorURL1 != null && nodeID1 != null && endpointID1 != null) {
			this.registry = Registry.getRegistryInstance();
			this.nodeID = nodeID1;
			this.creatorURL = creatorURL1;
			this.endpointID = endpointID1;
			//this.creationMode = 1;
			this.checkValidity(); 
		} else {
			throw new IllegalArgumentException(
					"ThreadProxy.<init> with creatorURL=" + creatorURL1
							+ ", url=" + url + ", nodeID=" + nodeID1
							+ ", endpointID=" + endpointID1);
		}
	}

	/**
	 * Creates a Proxy for the <code>jchordlocal</code> protocol. The host name
	 * part of {@link URL url}is the name of the node in the
	 * <code>oclocal</code> protocol.
	 * 
	 * @param creatorURL1
	 * 
	 * @param url
	 *            The {@link URL}of the node this proxy represents.
	 * @throws CommunicationException
	 */
	public ThreadProxy(URL creatorURL1, URL url) throws CommunicationException {
		super(url);
		this.registry = Registry.getRegistryInstance();
		this.creatorURL = creatorURL1;
		logger.debug("Trying to get id of node.");
		ThreadEndpoint endpoint_ = this.registry.lookup(this.nodeURL);
		logger.debug("Found endpoint " + endpoint_);
		if (endpoint_ == null) {
			throw new CommunicationException();
		}
		this.nodeID = endpoint_.getNodeID();
		this.endpointID = endpoint_.getEndpointID();
		//this.creationMode = 2;
	}

	void reSetNodeID(ID id) {
		this.setNodeID(id);
	}

	/**
	 * Method to check if this proxy is valid.
	 * 
	 * @throws CommunicationException
	 */
	private synchronized ThreadEndpoint checkValidity()
			throws CommunicationException {

		if (!this.isValid) {
			throw new CommunicationException("No valid connection! "
					+ this.creatorURL + " -> " + this.nodeURL
					+ " for Endpoint id: " + this.endpointID);
		}

		ThreadEndpoint ep = this.registry.lookup(this.nodeURL);
		if (ep == null) {
			this.isValid = false;
			throw new CommunicationException("No Endpoint found! "
					+ this.creatorURL + " -> " + this.nodeURL
					+ " for Endpoint id: " + this.endpointID);
		}

		if (!this.endpointID.equals(ep.getEndpointID())) {
			// the endpointID is not the same and therefore the endpoint
			// is not the current one
			this.isValid = false;
			String message = "No Endpoint found! " + this.creatorURL + " -> "
					+ this.nodeURL + " for Endpoint id: " + this.endpointID
					+ " and remote Endpoint id: " + ep.getEndpointID();
			throw new CommunicationException(message);

		}

		// the endpoint ID matches that of the endpoint.
		ep.registerProxy(this);
		if (!this.hasBeenUsed) {
			this.hasBeenUsed = true;
			Registry.getRegistryInstance()
					.addProxyUsedBy(this.creatorURL, this);
		}
		return ep;

		/*
		 * Ensure that node id is set, if has not been set before.
		 */
	}

	/**
	 * Test if this Proxy is valid.
	 * 
	 * @return <code>true</code> if this Proxy is valid.
	 */
	public boolean isValid() {
		return this.isValid;
	}

	/**
	 * Invalidates this proxy.
	 * 
	 */
	public synchronized void invalidate() {
		this.isValid = false;
	}

	/**
	 * Get a reference to the {@link ThreadEndpoint endpoint} this proxy
	 * delegates methods to. If there is no endpoint a
	 * {@link CommunicationException exception} is thrown.
	 * 
	 * @return Reference to the {@link ThreadEndpoint endpoint} this proxy
	 *         delegates methods to.
	 * @throws CommunicationException
	 *             If there is no endpoint this exception is thrown.
	 */
	public ThreadEndpoint getEndpoint() throws CommunicationException {
		ThreadEndpoint ep = this.registry.lookup(this.nodeURL);
		if (ep == null) {
			throw new CommunicationException();
		}
		return ep;
	}

	public Node findSuccessor(ID key) throws CommunicationException {
		ThreadEndpoint ep = this.checkValidity();
		Node succ = ep.findSuccessor(key);
		try {
			logger.debug("Creating clone of proxy " + succ);
			ThreadProxy temp = (ThreadProxy) succ;
			logger.debug("Clone created");
			return temp.cloneMeAt(this.creatorURL);
		} catch (Throwable t) {
			logger.debug("Exception during clone of proxy.", t);
			throw new CommunicationException(t);
		}
	}

	public void insertEntry(Entry entry) throws CommunicationException {
		ThreadEndpoint ep = this.checkValidity();
		logger.debug("Trying to execute insert().");
		logger.debug("Found endpoint " + ep);
		ep.insertEntry(entry);
		logger.debug("insert() executed");
	}

	public void removeEntry(Entry entry) throws CommunicationException {
		ThreadEndpoint ep = this.checkValidity();
		ep.removeEntry(entry);
	}

	/**
	 * 
	 */
	public String toString() {
		StringBuilder buffer = new StringBuilder();
		buffer.append("[ThreadProxy ");
		buffer.append(this.nodeURL);
		buffer.append("]");
		return buffer.toString();
	}

	public List<Node> notify(Node potentialPredecessor)
			throws CommunicationException {
		ThreadEndpoint ep = this.checkValidity();

		ThreadProxy potentialPredecessorProxy = null;
		if (potentialPredecessor instanceof ThreadProxy) {
			potentialPredecessorProxy = ((ThreadProxy) potentialPredecessor)
					.cloneMeAt(this.creatorURL);
		} else {
			potentialPredecessorProxy = new ThreadProxy(this.creatorURL,
					potentialPredecessor.getNodeURL(), Endpoint.getEndpoint(
							this.creatorURL).getNode().getNodeID(),
					((ThreadEndpoint) Endpoint.getEndpoint(this.creatorURL))
							.getEndpointID());
		}

		logger.debug("Trying to execute notify().");
		logger.debug("Found endpoint " + ep);
		List<Node> nodes = ep.notify(potentialPredecessorProxy);
		Node[] proxies = new Node[nodes.size()];
		try {
			int currentIndex = 0;
			for (Iterator<Node> i = nodes.iterator(); i.hasNext();) {
				Object o = i.next();
				ThreadProxy current = (ThreadProxy) o;
				proxies[currentIndex++] = current.cloneMeAt(this.creatorURL);
			}
		} catch (Throwable t) {
			throw new CommunicationException(t);
		}
		return Arrays.asList(proxies);
	}

	public void ping() throws CommunicationException {
		ThreadEndpoint ep = this.checkValidity();
		logger.debug("Trying to execute ping().");
		logger.debug("Found endpoint " + ep);
		ep.ping();
	}

	public Set<Entry> retrieveEntries(ID id) throws CommunicationException {
		ThreadEndpoint ep = this.checkValidity();
		logger.debug("Trying to execute retrieve().");
		logger.debug("Found endpoint " + ep);
		return ep.retrieveEntries(id);
	}

	/**
	 * Creates a copy of this.
	 * 
	 * @param creatorUrl
	 *            The url of the node where this is being copied.
	 * @return The copy of this.
	 */
	ThreadProxy cloneMeAt(URL creatorUrl) throws CommunicationException {
		return new ThreadProxy(creatorUrl, this.nodeURL, this.nodeID,
				this.endpointID);
	}

	public void leavesNetwork(Node predecessor) throws CommunicationException {
		ThreadEndpoint ep = this.checkValidity();

		ThreadProxy predecessorProxy = null;
		if (predecessor instanceof ThreadProxy) {
			predecessorProxy = ((ThreadProxy) predecessor)
					.cloneMeAt(this.creatorURL);
		} else {
			predecessorProxy = new ThreadProxy(this.creatorURL, predecessor
					.getNodeURL(), Endpoint.getEndpoint(this.creatorURL)
					.getNode().getNodeID(), ((ThreadEndpoint) Endpoint
					.getEndpoint(this.creatorURL)).getEndpointID());
		}

		logger.debug("Trying to execute leavesNetwork(" + predecessor + ").");
		logger.debug("Found endpoint " + ep);
		ep.leavesNetwork(predecessorProxy);
	}

	public void removeReplicas(ID sendingNodeID, Set<Entry> entriesToRemove)
			throws CommunicationException {
		ThreadEndpoint ep = this.checkValidity();
		logger.debug("Trying to execute removeReplicas(" + entriesToRemove
				+ ").");
		logger.debug("Found endpoint " + ep);
		ep.removeReplicas(sendingNodeID, entriesToRemove);
	}

	public void insertReplicas(Set<Entry> entries)
			throws CommunicationException {
		ThreadEndpoint ep = this.checkValidity();
		logger.debug("Trying to execute insertReplicas(" + entries + ").");
		logger.debug("Found endpoint " + ep);
		ep.insertReplicas(entries);
	}

	public RefsAndEntries notifyAndCopyEntries(Node potentialPredecessor)
			throws CommunicationException {
		ThreadEndpoint ep = this.checkValidity();
		ThreadProxy potentialPredecessorProxy = null;
		if (potentialPredecessor instanceof ThreadProxy) {
			potentialPredecessorProxy = ((ThreadProxy) potentialPredecessor)
					.cloneMeAt(this.creatorURL);
		} else {
			potentialPredecessorProxy = new ThreadProxy(this.creatorURL,
					potentialPredecessor.getNodeURL(), Endpoint.getEndpoint(
							this.creatorURL).getNode().getNodeID(),
					((ThreadEndpoint) Endpoint.getEndpoint(this.creatorURL))
							.getEndpointID());
		}

		logger.debug("Trying to execute notifyAndCopyEntries().");
		logger.debug("Found endpoint " + ep);
		RefsAndEntries refs = ep
				.notifyAndCopyEntries(potentialPredecessorProxy);
		List<Node> nodes = refs.getRefs();
		Node[] proxies = new Node[nodes.size()];
		try {
			int currentIndex = 0;
			for (Iterator<Node> i = nodes.iterator(); i.hasNext();) {
				Object o = i.next();
				ThreadProxy current = (ThreadProxy) o;
				proxies[currentIndex++] = current.cloneMeAt(this.creatorURL);
			}
		} catch (Throwable t) {
			throw new CommunicationException(t);
		}
		RefsAndEntries result = new RefsAndEntries(Arrays.asList(proxies), refs
				.getEntries());
		return result;
	}

}