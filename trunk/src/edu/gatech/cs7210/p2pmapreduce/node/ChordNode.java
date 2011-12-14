package edu.gatech.cs7210.p2pmapreduce.node;

import java.io.Serializable;
import java.util.Set;

import de.uniba.wiai.lspi.chord.data.URL;
import de.uniba.wiai.lspi.chord.service.Chord;
import de.uniba.wiai.lspi.chord.service.ServiceException;
import de.uniba.wiai.lspi.chord.service.impl.ChordImpl;
import edu.gatech.cs7210.p2pmapreduce.ApplicationContext;
import edu.gatech.cs7210.p2pmapreduce.chord.StringKey;
import edu.gatech.cs7210.p2pmapreduce.task.ITask;

public class ChordNode {
	
	//private static final String MASTER_URL = "http://50.57.191.73:9000";
	private static final String MASTER_URL = "http://localhost:9000";
	
	private Chord chord = new ChordImpl();
	
	public void runAsFirst() {
		try {
			chord.create(ApplicationContext.getInstance().getUrl());
		} catch (ServiceException e) {
			System.err.println("Could not create Chord service");
			e.printStackTrace();
		}
	}
	
	public void run(URL bootstrapUrl) {
		try {
			chord.join(ApplicationContext.getInstance().getUrl(), bootstrapUrl);
		} catch (ServiceException e) {
			System.err.println("Could not create Chord service");
			e.printStackTrace();
		}
	}
	
	public String publishAsSlave() {
		try {
			// find master to join
			Set<Serializable> results = chord.retrieve(new StringKey(MASTER_URL));
			if (results.size() == 0) {
				System.err.println("Failed to find master on chord topology");
				System.exit(-1);
			}
			return results.iterator().next().toString();
		} catch (ServiceException e) {
			System.err.println("Failed to publish as slave");
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}
	
	public void publishAsMaster() {
		try {
			chord.insert(new StringKey(MASTER_URL), ApplicationContext.getInstance().getUrl());
		} catch (ServiceException e) {
			System.err.println("Failed to publish as slave");
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public void publishTask(ITask task) {
		
	}
}
