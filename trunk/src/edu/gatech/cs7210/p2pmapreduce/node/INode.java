package edu.gatech.cs7210.p2pmapreduce.node;

import de.uniba.wiai.lspi.chord.data.URL;

public interface INode {

	public void run();
	
	public boolean update(URL url);
}
