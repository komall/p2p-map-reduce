package edu.gatech.cs7210.p2pmapreduce.node;

import java.io.File;

import de.uniba.wiai.lspi.chord.data.URL;
import edu.gatech.cs7210.p2pmapreduce.ApplicationContext;
import edu.gatech.cs7210.p2pmapreduce.task.ITask;

public class SlaveNode extends AbstractNode {

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
	
	public boolean run() {
		return executeCommand(
				ApplicationContext.getInstance().getBinDir() + File.separator + "slaves.sh");
	}
	
	public boolean update(URL url) {
		return false;
	}
	
	public boolean executeTask(ITask task) {
		return false;
	}
}
