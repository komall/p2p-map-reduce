package edu.gatech.cs7210.p2pmapreduce.node;

public class MasterNode implements INode {

	public enum MasterType {
		NAME_NODE, JOB_TRACKER
	}
	
	public void run() {
		// execute Hadoop command line process with correct parameters
		
	}
	
	public void updateWorkerConfiguration() {
		// update the Worker configuration file to include all Workers present
		// in the Chord overlay which are delegated to this Master server
		
	}
}
