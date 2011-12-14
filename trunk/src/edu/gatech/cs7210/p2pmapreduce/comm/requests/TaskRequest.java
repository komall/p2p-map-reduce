package edu.gatech.cs7210.p2pmapreduce.comm.requests;

import de.uniba.wiai.lspi.chord.data.URL;
import edu.gatech.cs7210.p2pmapreduce.task.ITask;

public class TaskRequest extends IRequest {

	private ITask task;
	private URL url;
	
	public TaskRequest(ITask task, URL url) {
		this.task = task;
		this.url = url;
	}
	
	@Override
	public URL getUrl() {
		return this.url;
	}
	
	public ITask getTask() {
		return this.task;
	}
}
