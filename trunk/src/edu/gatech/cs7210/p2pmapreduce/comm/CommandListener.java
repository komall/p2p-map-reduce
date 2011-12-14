package edu.gatech.cs7210.p2pmapreduce.comm;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import de.uniba.wiai.lspi.chord.data.URL;
import edu.gatech.cs7210.p2pmapreduce.ApplicationContext;
import edu.gatech.cs7210.p2pmapreduce.comm.requests.IRequest;
import edu.gatech.cs7210.p2pmapreduce.comm.requests.JoinRequest;
import edu.gatech.cs7210.p2pmapreduce.comm.responses.IResponse;
import edu.gatech.cs7210.p2pmapreduce.comm.responses.JoinResponse;
import edu.gatech.cs7210.p2pmapreduce.node.INode;
import edu.gatech.cs7210.p2pmapreduce.node.MasterNode;

public class CommandListener {

	private ServerSocket server;
	
	public void listen(URL url) {
		try {
			server = new ServerSocket(url.getPort());
			System.out.println("listening on port: " + url.getPort());
		} catch (IOException e) {
			System.err.println("Error listening on port: " + url.getPort());
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		
		while(true) {
			try {
				Socket client = server.accept();
				ObjectInputStream is = new ObjectInputStream(client.getInputStream());
				ObjectOutputStream os = new ObjectOutputStream(client.getOutputStream());
				
				IRequest request = (IRequest)is.readObject();
				IResponse response = request.handleRequest(this);
				
			} catch (IOException e) {
				System.out.println("Accept failed: " + url.getPort());
				System.exit(-1);
			} catch (ClassNotFoundException e) {
				System.out.println("Failed to parse request object");
				System.exit(-1);
			}
		}
	}
	
	protected void finalize() {
		try {
			server.close();
		} catch (IOException e) {
			System.out.println("Could not close socket.");
			System.exit(-1);
		}
	}
	
	public IResponse handleRequest(IRequest request) {
		// TODO: should not be needed, due to dynamic invocation
		return null;
	}
	
	public IResponse handleRequest(JoinRequest request) {
		if (!ApplicationContext.getInstance().isMaster()) {
			return new JoinResponse(false);
		}
		INode node = ApplicationContext.getInstance().getNode();
		node.update(request.getUrl());
		return new JoinResponse(true);
	}
}
