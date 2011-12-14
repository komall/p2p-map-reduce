package edu.gatech.cs7210.p2pmapreduce.node;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
	
	public boolean update(URL url) {
		try {
			shutdownMaster();
			updateSlaveConfiguration(url);
			// TODO: rather than wait an arbitrary amount of time for the master to shutdown,
			// 	     read the processes input stream and determine success or failure and the
			//       point at which the process terminates
			Thread.sleep(20000);
			run();
			return true;
		} catch (InterruptedException e) {
			System.err.println("Master interrupted during update");
			e.printStackTrace();
			System.exit(-1);
		}
		return false;
	}
	
	public void shutdownMaster() {
		try {
			Process p = Runtime.getRuntime().exec(
					ApplicationContext.getInstance().getBinDir() + File.separator + "stop-all.sh");
		} catch (IOException e) {
			System.err.println("Failed to shutdown master");
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	private void updateSlaveConfiguration(URL url) {
		// update the Slave configuration file to include all Workers present
		// in the Chord overlay which are delegated to this Master server
		try {
			ApplicationContext appContext = ApplicationContext.getInstance();
			File slaveConfigFile = new File(appContext.getConfigDir() + 
					File.separator + appContext.getSlaveConfigFile());
			BufferedWriter writer = new BufferedWriter(new FileWriter(slaveConfigFile, true));
			writer.write("\n" + url.getPath());
			writer.close();
		} catch (IOException e) {
			System.err.println("Failed to update slave config file");
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
