/***************************************************************************
 *                                                                         *
 *                               Entries.java                              *
 *                            -------------------                          *
 *   date                 : 07.01.2008                                     *
 *   copyright            : (C) 2004-2008 Distributed and                  *
 *                              Mobile Systems Group                       *
 *                              Lehrstuhl fuer Praktische Informatik       *
 *                              Universitaet Bamberg                       *
 *                              http://www.uni-bamberg.de/pi/              *
 *                          (C) 2008 Martin Kihlgren                       *
 *   email                : sven.kaffille@uni-bamberg.de                   *
 *   			            karsten.loesing@uni-bamberg.de                 *
 *                          martin at troja dot ath dot cx                 *
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

package de.uniba.wiai.lspi.chord.service.impl;

import java.io.*;
import java.util.*;

import de.uniba.wiai.lspi.chord.data.Entry;
import de.uniba.wiai.lspi.chord.data.ID;

/**
 * Stores entries for the local node in files on disk and provides methods for
 * accessing them. It IS allowed, that multiple objects of type {@link Entry}
 * with same {@link ID} are stored!
 * 
 * This is an incredibly silly thing to do, in many ways, and is only meant to
 * be a proof of concept and example of how custom Entries implementations can
 * work.
 * 
 * @author Karsten Loesing, Sven Kaffille, Martin Kihlgren
 * @version 1.0.5
 * 
 */

final class SerializedDiskEntries extends Entries {

	private File directory;

	public SerializedDiskEntries() {
		String propertyName = SerializedDiskEntries.class.getName()
				+ ".Directory";
		if (System.getProperty(propertyName) == null) {
			throw new RuntimeException(
					"You must define the "
							+ propertyName
							+ " property if you want to use the SerializedDiskEntries class!");
		} else {
			this.directory = new File(System.getProperty(propertyName));
			if (!this.directory.exists() || !this.directory.isDirectory()) {
				throw new RuntimeException(this.directory
						+ " does not exist or is not a directory!");
			}
		}
		logger.info("SerializedDiskEntries initialized with directory "
				+ directory);
	}

	/**
	 * Returns whether there exists a file denoted by the id.
	 */
	private boolean hasEntries(ID id) {
		return new File(directory, id.toHexString()).exists();
	}

	/**
	 * Loads a Set<Entry> from the file denoted by the id.
	 */
	@SuppressWarnings("unchecked")
	private Set<Entry> loadEntries(ID id) {
		try {
			return (Set<Entry>) new ObjectInputStream(new FileInputStream(
					new File(directory, id.toHexString()))).readObject();
		} catch (IOException e) {
			throw new RuntimeException("" + e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("" + e);
		}
	}

	/**
	 * Dumps the entries into the file denoted by the id.
	 */
	private void saveEntries(ID id, Set<Entry> entries) {
		try {
			new ObjectOutputStream(new FileOutputStream(new File(directory, id
					.toHexString()))).writeObject(entries);
		} catch (IOException e) {
			throw new RuntimeException("" + e);
		}
	}

	/**
	 * Removes the file denoted by the id.
	 */
	private void removeEntries(ID id) {
		new File(directory, id.toHexString()).delete();
	}

	/**
	 * Returns a Collection<ID> containing all ID's that the files in our
	 * directory signify.
	 */
	private Collection<ID> getIDs() {
		ArrayList<ID> returnValue = new ArrayList<ID>();
		String[] fileNames = directory.list();
		for (int i = 0; i < fileNames.length; i++) {
			String[] byteValues = fileNames[i].split(" ");
			byte[] bytes = new byte[byteValues.length];
			for (int j = 0; j < byteValues.length; j++) {
				bytes[j] = (byte) Integer.parseInt(byteValues[j], 16);
			}
			returnValue.add(new ID(bytes));
		}
		return returnValue;
	}

	/**
	 * Stores a set of entries to the local disk.
	 * 
	 * @param entriesToAdd
	 *            Set of entries to add to the repository.
	 * @throws NullPointerException
	 *             If set reference is <code>null</code>.
	 */
	final void addAll(Set<Entry> entriesToAdd) {

		if (entriesToAdd == null) {
			NullPointerException e = new NullPointerException(
					"Set of entries to be added to the local hash table may "
							+ "not be null!");
			Entries.logger.error("Null pointer", e);
			throw e;
		}

		for (Entry nextEntry : entriesToAdd) {
			this.add(nextEntry);
		}

		if (debugEnabled) {
			Entries.logger.debug("Set of entries of length "
					+ entriesToAdd.size() + " was added.");
		}
	}

	/**
	 * Stores one entry to the local disk.
	 * 
	 * @param entryToAdd
	 *            Entry to add to the repository.
	 * @throws NullPointerException
	 *             If entry to add is <code>null</code>.
	 */
	final void add(Entry entryToAdd) {

		if (entryToAdd == null) {
			NullPointerException e = new NullPointerException(
					"Entry to add may not be null!");
			Entries.logger.error("Null pointer", e);
			throw e;
		}

		Set<Entry> values;
		synchronized (this) {
			if (hasEntries(entryToAdd.getId())) {
				values = loadEntries(entryToAdd.getId());
			} else {
				values = new HashSet<Entry>();
			}
			values.add(entryToAdd);
			saveEntries(entryToAdd.getId(), values);
		}
		if (debugEnabled) {
			Entries.logger.debug("Entry was added: " + entryToAdd);
		}
	}

	/**
	 * Removes the given entry from the local hash table.
	 * 
	 * @param entryToRemove
	 *            Entry to remove from the hash table.
	 * @throws NullPointerException
	 *             If entry to remove is <code>null</code>.
	 */
	final void remove(Entry entryToRemove) {

		if (entryToRemove == null) {
			NullPointerException e = new NullPointerException(
					"Entry to remove may not be null!");
			Entries.logger.error("Null pointer", e);
			throw e;
		}

		synchronized (this) {
			if (hasEntries(entryToRemove.getId())) {
				Set<Entry> values = loadEntries(entryToRemove.getId());
				values.remove(entryToRemove);
				if (values.size() == 0) {
					removeEntries(entryToRemove.getId());
				} else {
					saveEntries(entryToRemove.getId(), values);
				}
			}
		}
		if (debugEnabled) {
			Entries.logger.debug("Entry was removed: " + entryToRemove);
		}
	}

	/**
	 * Returns a set of entries matching the given ID. If no entries match the
	 * given ID, an empty set is returned.
	 * 
	 * @param id
	 *            ID of entries to be returned.
	 * @throws NullPointerException
	 *             If given ID is <code>null</code>.
	 * @return Set of matching entries. Empty Set if no matching entries are
	 *         available.
	 */
	final Set<Entry> getEntries(ID id) {

		if (id == null) {
			NullPointerException e = new NullPointerException(
					"ID to find entries for may not be null!");
			Entries.logger.error("Null pointer", e);
			throw e;
		}
		synchronized (this) {
			/*
			 * This has to be synchronized as the test if the map contains a set
			 * associated with id can succeed and then the thread may hand
			 * control over to another thread that removes the Set belonging to
			 * id. In that case this.entries.get(id) would return null which
			 * would break the contract of this method.
			 */
			if (hasEntries(id)) {
				Set<Entry> entriesForID = loadEntries(id);
				if (debugEnabled) {
					Entries.logger.debug("Returning entries " + entriesForID);
				}
				return entriesForID;
			}
			if (debugEnabled) {
				Entries.logger.debug("No entries available for " + id
						+ ". Returning empty set.");
			}
			return new HashSet<Entry>();
		}

	}

	/**
	 * Returns all entries in interval, excluding lower bound, but including
	 * upper bound
	 * 
	 * @param fromID
	 *            Lower bound of IDs; entries matching this ID are NOT included
	 *            in result.
	 * @param toID
	 *            Upper bound of IDs; entries matching this ID ARE included in
	 *            result.
	 * @throws NullPointerException
	 *             If either or both of the given ID references have value
	 *             <code>null</code>.
	 * @return Set of matching entries.
	 */
	final Set<Entry> getEntriesInInterval(ID fromID, ID toID) {

		if (fromID == null || toID == null) {
			NullPointerException e = new NullPointerException(
					"Neither of the given IDs may have value null!");
			Entries.logger.error("Null pointer", e);
			throw e;
		}

		Set<Entry> result = new HashSet<Entry>();

		synchronized (this) {
			for (ID nextID : getIDs()) {
				if (nextID.isInInterval(fromID, toID)) {
					Set<Entry> entriesForID = loadEntries(nextID);
					for (Entry entryToAdd : entriesForID) {
						result.add(entryToAdd);
					}
				}
			}
		}

		// add entries matching upper bound
		result.addAll(this.getEntries(toID));

		return result;
	}

	/**
	 * Removes the given entries from the local hash table.
	 * 
	 * @param toRemove
	 *            Set of entries to remove from local hash table.
	 * @throws NullPointerException
	 *             If the given set of entries is <code>null</code>.
	 */
	final void removeAll(Set<Entry> toRemove) {

		if (toRemove == null) {
			NullPointerException e = new NullPointerException(
					"Set of entries may not have value null!");
			Entries.logger.error("Null pointer", e);
			throw e;
		}

		for (Entry nextEntry : toRemove) {
			this.remove(nextEntry);
		}

		if (debugEnabled) {
			Entries.logger.debug("Set of entries of length " + toRemove.size()
					+ " was removed.");
		}
	}

	/**
	 * Returns an unmodifiable map of all stored entries.
	 * 
	 * @return Unmodifiable map of all stored entries.
	 */
	final Map<ID, Set<Entry>> getEntries() {
		Map<ID, Set<Entry>> returnValue = new HashMap<ID, Set<Entry>>();

		for (ID nextID : getIDs()) {
			returnValue.put(nextID, loadEntries(nextID));
		}
		return Collections.unmodifiableMap(returnValue);
	}

	/**
	 * Returns the number of stored entries.
	 * 
	 * @return Number of stored entries.
	 */
	final int getNumberOfStoredEntries() {
		return getIDs().size();
	}

	/**
	 * Returns a formatted string of all entries stored in the local hash table.
	 * 
	 * @return String representation of all stored entries.
	 */
	public final String toString() {
		StringBuilder result = new StringBuilder("Entries:\n");
		for (ID nextId : getIDs()) {
			for (Entry entry : loadEntries(nextId)) {
				result.append("  " + entry.toString());
			}
		}
		return result.toString();
	}
}
