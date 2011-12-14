package edu.gatech.cs7210.p2pmapreduce.comm.responses;

public class JoinResponse implements IResponse {

	private boolean success = false;
	
	public JoinResponse(boolean success) {
		this.success = success;
	}
	
	public boolean isSuccess() {
		return this.success;
	}
}
