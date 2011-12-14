package edu.gatech.cs7210.p2pmapreduce.node;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import de.uniba.wiai.lspi.chord.data.URL;
import edu.gatech.cs7210.p2pmapreduce.ApplicationContext;
import edu.gatech.cs7210.p2pmapreduce.task.ITask;

public class SlaveNode implements INode {

	public enum SlaveType {
		DATA_NODE, TASK_TRACKER
	}
	
	private SlaveType type;
	
	public SlaveNode(SlaveType type) {
		this.type = type;
	}
	
	public SlaveType getType() {
		return this.type;
	}
	
	public boolean run() {
		try {
			Process p = Runtime.getRuntime().exec(
					ApplicationContext.getInstance().getBinDir() + File.separator + "slaves.sh");
			InputStream s = p.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(s));
			String line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				line = reader.readLine();
			}
			return true;
		} catch (IOException e) {
			System.err.println("Failed to run slave");
			e.printStackTrace();
			System.exit(-1);
		}
		return false;
	}
	
	public boolean update(URL url) {
		return false;
	}
	
	public boolean executeTask(ITask task) {
		return false;
	}
}
