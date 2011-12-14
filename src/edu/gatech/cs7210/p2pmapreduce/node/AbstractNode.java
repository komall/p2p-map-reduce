package edu.gatech.cs7210.p2pmapreduce.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public abstract class AbstractNode implements INode {
	
	protected boolean executeCommand(String command) {
		try {
			Process p = Runtime.getRuntime().exec(command);
			InputStream s = p.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(s));
			String line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				line = reader.readLine();
			}
			return true;
		} catch (IOException e) {
			System.err.println("Failed to execute task [" + command + "]");
			e.printStackTrace();
			System.exit(-1);
		}
		return false;
		
	}
}
