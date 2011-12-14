package edu.gatech.cs7210.p2pmapreduce.comm;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import edu.gatech.cs7210.p2pmapreduce.comm.requests.IRequest;
import edu.gatech.cs7210.p2pmapreduce.comm.responses.IResponse;

public class CommandDispatcher {

	public void dispatch(IRequest request) {

		try {
			Socket client = new Socket(request.getUrl().getHost(), request.getUrl().getPort());
			
			ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(client.getInputStream());
			
			oos.writeObject(request);
			
			IResponse response = (IResponse)ois.readObject();
			if (response.isSuccess() == false) {
				System.err.println("Failed to send request to [" + request.getUrl().getPath() + "]");
			}
			
		} catch (IOException e) {
			System.err.println("Failed to write IRequest to ObjectOutputStream");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.err.println("Failed to receive IResponse object from CommandListener");
			e.printStackTrace();
		}
	}
}
