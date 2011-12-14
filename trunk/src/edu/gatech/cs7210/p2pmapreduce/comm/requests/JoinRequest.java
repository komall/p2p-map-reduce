package edu.gatech.cs7210.p2pmapreduce.comm.requests;

import de.uniba.wiai.lspi.chord.data.URL;

public class JoinRequest extends IRequest {

	private URL url;
	
	public JoinRequest(URL url) {
		this.url = url;
	}
	
	public URL getUrl() {
		return this.url;
	}
}
