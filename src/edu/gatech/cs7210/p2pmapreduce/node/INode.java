package edu.gatech.cs7210.p2pmapreduce.node;

import de.uniba.wiai.lspi.chord.data.URL;
import edu.gatech.cs7210.p2pmapreduce.task.ITask;

public interface INode {

	public boolean run();
	
	public boolean update(URL url);
	
	public boolean executeTask(ITask task);
}
