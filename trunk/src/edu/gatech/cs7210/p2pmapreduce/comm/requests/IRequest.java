package edu.gatech.cs7210.p2pmapreduce.comm.requests;

import de.uniba.wiai.lspi.chord.data.URL;
import edu.gatech.cs7210.p2pmapreduce.comm.CommandListener;
import edu.gatech.cs7210.p2pmapreduce.comm.responses.IResponse;

public abstract class IRequest {

	public abstract URL getUrl();

	public IResponse handleRequest(CommandListener listener) {
		return listener.handleRequest(this);
	}
}
