package edu.gatech.cs7210.p2pmapreduce.node;

import de.uniba.wiai.lspi.chord.data.URL;
import de.uniba.wiai.lspi.chord.service.Chord;
import de.uniba.wiai.lspi.chord.service.ServiceException;
import de.uniba.wiai.lspi.chord.service.impl.ChordImpl;
import edu.gatech.cs7210.p2pmapreduce.ApplicationContext;

public class ChordNode {
	
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
}
