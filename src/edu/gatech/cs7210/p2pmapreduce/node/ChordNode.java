package edu.gatech.cs7210.p2pmapreduce.node;

import de.uniba.wiai.lspi.chord.data.URL;
import de.uniba.wiai.lspi.chord.service.Chord;
import de.uniba.wiai.lspi.chord.service.ServiceException;
import de.uniba.wiai.lspi.chord.service.impl.ChordImpl;
import edu.gatech.cs7210.p2pmapreduce.ApplicationContext;
import edu.gatech.cs7210.p2pmapreduce.chord.UrlKey;
import edu.gatech.cs7210.p2pmapreduce.comm.CommandDispatcher;
import edu.gatech.cs7210.p2pmapreduce.comm.requests.TaskRequest;
import edu.gatech.cs7210.p2pmapreduce.task.ITask;

public class ChordNode {
	
	private Chord chord;
	
	public ChordNode() {
		chord = new ChordImpl();
	}
	
	public void runAsFirst() {
		try {
			chord.create(ApplicationContext.getInstance().getUrl());
		} catch (ServiceException e) {
			System.err.println("Could not create Chord service");
			e.printStackTrace();
		}
	}
	
	public URL run(URL bootstrapUrl, boolean isMaster) {
		try {
			// join chord topology as either a master or slave node
			return chord.join(ApplicationContext.getInstance().getUrl(), bootstrapUrl, isMaster);
		} catch (ServiceException e) {
			System.err.println("Could not create Chord service");
			e.printStackTrace();
		}
		return null;
	}
	
	public void publishTask(ITask task) {
		try {
			// insert into chord topology to find responsible master node
			URL masterUrl = chord.insert(new UrlKey(ApplicationContext.getInstance().getUrl()), ApplicationContext.getInstance().getUrl());
			
			// submit task to master
			CommandDispatcher dispatcher = new CommandDispatcher();
			dispatcher.dispatch(new TaskRequest(task, masterUrl));
			
		} catch (ServiceException e) {
			System.err.println("Could not create Chord service");
			e.printStackTrace();
		}
	}
}
