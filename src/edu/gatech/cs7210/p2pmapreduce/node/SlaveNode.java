package edu.gatech.cs7210.p2pmapreduce.node;

import java.io.File;
import java.io.IOException;

import de.uniba.wiai.lspi.chord.data.URL;
import edu.gatech.cs7210.p2pmapreduce.ApplicationContext;

public class SlaveNode implements INode {

	public enum SlaveType {
		DATA_NODE, TASK_TRACKER
	}
	
	private SlaveType type;
	
	public SlaveNode(SlaveType type) {
		this.type = type;
	}
	
	public SlaveType getType() {
		return this.type;
	}
	
	public void run() {
		try {
			Process p = Runtime.getRuntime().exec(
					ApplicationContext.getInstance().getBinDir() + File.separator + "slaves.sh");
		} catch (IOException e) {
			System.err.println("Failed to run slave");
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public boolean update(URL url) {
		return true;
	}
}
