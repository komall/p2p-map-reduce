package edu.gatech.cs7210.p2pmapreduce.node;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import de.uniba.wiai.lspi.chord.data.URL;
import edu.gatech.cs7210.p2pmapreduce.ApplicationContext;
import edu.gatech.cs7210.p2pmapreduce.task.ITask;

public class MasterNode extends AbstractNode {

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
	
	public boolean run() {
		return executeCommand(
				ApplicationContext.getInstance().getBinDir() + File.separator + "start-all.sh");
	}
	
	public boolean update(URL url) {
		shutdownMaster();
		updateSlaveConfiguration(url);
		return run();
	}
	
	public boolean executeTask(ITask task) {
		return executeCommand(task.getCommand());
	}
	
	public boolean shutdownMaster() {
		return executeCommand(
				ApplicationContext.getInstance().getBinDir() + File.separator + "stop-all.sh");
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
