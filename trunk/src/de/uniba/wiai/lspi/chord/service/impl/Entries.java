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

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;

import de.uniba.wiai.lspi.chord.data.Entry;
import de.uniba.wiai.lspi.chord.data.ID;
import de.uniba.wiai.lspi.chord.service.Chord;
import de.uniba.wiai.lspi.util.logging.Logger;

/**
 * Defines an interface that stores entries for the local node and provides
 * methods for accessing them. It IS allowed, that multiple objects of type
 * {@link Entry} with same {@link ID} are stored!
 * 
 * @author Karsten Loesing, Sven Kaffille, Martin Kihlgren
 * @version 1.0.5
 * 
 */

abstract class Entries {

	@SuppressWarnings({ "unchecked", "cast" })
	static Entries getInstance(Chord chord) {
		String className = System.getProperty(Entries.class.getName()
				+ ".ClassName");
		if (className != null) {
			Class<Entries> klass;
			try {
				klass = (Class<Entries>)Class.forName(className);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("" + e);
			}
			try {
				try {
					return (Entries) klass.getConstructor(Chord.class)
							.newInstance(chord);
				} catch (NoSuchMethodException e) {
					return (Entries) klass.newInstance();
				}
			} catch (InstantiationException e) {
				throw new RuntimeException("Could not instantiate Entries class!", e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Could not instantiate Entries class!", e);
			} catch (InvocationTargetException e) {
				throw new RuntimeException("Could not instantiate Entries class!", e);
			}
		} else {
			return new TreeMapEntries();
		}
	}

	/**
	 * Object logger.
	 */
	protected final static Logger logger = Logger.getLogger(Entries.class);

	protected final static boolean debugEnabled = logger
			.isEnabledFor(Logger.LogLevel.DEBUG);

	/**
	 * Stores a set of entries to the local hash table.
	 * 
	 * @param entriesToAdd
	 *            Set of entries to add to the repository.
	 * @throws NullPointerException
	 *             If set reference is <code>null</code>.
	 */
	abstract void addAll(Set<Entry> entriesToAdd);

	/**
	 * Stores one entry to the local hash table.
	 * 
	 * @param entryToAdd
	 *            Entry to add to the repository.
	 * @throws NullPointerException
	 *             If entry to add is <code>null</code>.
	 */
	abstract void add(Entry entryToAdd);

	/**
	 * Removes the given entry from the local hash table.
	 * 
	 * @param entryToRemove
	 *            Entry to remove from the hash table.
	 * @throws NullPointerException
	 *             If entry to remove is <code>null</code>.
	 */
	abstract void remove(Entry entryToRemove);

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
	abstract Set<Entry> getEntries(ID id);

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
	abstract Set<Entry> getEntriesInInterval(ID fromID, ID toID);

	/**
	 * Removes the given entries from the local hash table.
	 * 
	 * @param toRemove
	 *            Set of entries to remove from local hash table.
	 * @throws NullPointerException
	 *             If the given set of entries is <code>null</code>.
	 */
	abstract void removeAll(Set<Entry> toRemove);

	/**
	 * Returns an unmodifiable map of all stored entries.
	 * 
	 * @return Unmodifiable map of all stored entries.
	 */
	abstract Map<ID, Set<Entry>> getEntries();

	/**
	 * Returns the number of stored entries.
	 * 
	 * @return Number of stored entries.
	 */
	abstract int getNumberOfStoredEntries();

	/**
	 * Returns a formatted string of all entries stored in the local hash table.
	 * 
	 * @return String representation of all stored entries.
	 */
	abstract public String toString();
}