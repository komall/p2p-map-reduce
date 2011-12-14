package edu.gatech.cs7210.p2pmapreduce.comm.responses;

public class TaskResponse implements IResponse {

	private boolean success;
	
	public TaskResponse(boolean success) {
		this.success = success;
	}
	
	@Override
	public boolean isSuccess() {
		return this.success;
	}
}
