package edu.gatech.cs7210.p2pmapreduce.node;

import java.io.File;
import java.io.IOException;

import de.uniba.wiai.lspi.chord.data.URL;
import edu.gatech.cs7210.p2pmapreduce.ApplicationContext;

public class MasterNode implements INode {

	public enum MasterType {
		NAME_NODE, JOB_TRACKER
	}
	
	private MasterType type;
	
	public MasterNode(MasterType type) {
		this.type = type;
	}
	
	public MasterType getType() {
		return this.type;
	}
	
	public void run() {
		try {
			Process p = Runtime.getRuntime().exec(
					ApplicationContext.getInstance().getBinDir() + File.separator + "start-all.sh");
		} catch (IOException e) {
			System.err.println("Failed to run master");
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public void update(URL url) {
		updateSlaveConfiguration(url);
	}
	
	private void updateSlaveConfiguration(URL url) {
		// update the Slave configuration file to include all Workers present
		// in the Chord overlay which are delegated to this Master server
		
	}
}
