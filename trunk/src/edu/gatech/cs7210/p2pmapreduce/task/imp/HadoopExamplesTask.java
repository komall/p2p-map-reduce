package edu.gatech.cs7210.p2pmapreduce.task.imp;

import edu.gatech.cs7210.p2pmapreduce.task.ITask;

public class HadoopExamplesTask implements ITask {

	@Override
	public String getTaskName() {
		return "hadoop-examples";
	}
	
	@Override
	public String getCommand() {
		return "bin/hadoop jar hadoop-examples-*.jar grep input output 'dfs[a-z.]+'";
	}
}
