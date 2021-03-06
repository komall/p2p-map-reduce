package edu.gatech.cs7210.p2pmapreduce;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;

import de.uniba.wiai.lspi.chord.data.URL;
import edu.gatech.cs7210.p2pmapreduce.node.ChordNode;
import edu.gatech.cs7210.p2pmapreduce.node.INode;
import edu.gatech.cs7210.p2pmapreduce.node.MasterNode.MasterType;
import edu.gatech.cs7210.p2pmapreduce.node.SlaveNode.SlaveType;

public class ApplicationContext {
	
	private boolean firstNode = false;
	private String protocol = "http://";
	private ChordNode chordNode;
	private INode node;
	private URL bootstrapUrl;
	private MasterType masterType;
	private SlaveType slaveType;
	private String configDir;
	private String binDir;
	private String slaveConfigFile;
	private boolean isMaster = false;
	private boolean isSlave = false;
	private int mapReducePort;
	private int chordPort;

	private ApplicationContext() { }
	
	private static ApplicationContext INSTANCE = new ApplicationContext();
	
	public static ApplicationContext getInstance() {
		return INSTANCE;
	}
	
	public INode getNode() {
		return node;
	}

	public void setNode(INode node) {
		this.node = node;
	}

	public boolean isFirstNode() {
		return firstNode;
	}
	
	public void setFirstNode(boolean firstNode) {
		this.firstNode = firstNode;
	}
	
	public void setMapReducePort(int port) {
		this.mapReducePort = port;
	}
	
	public void setChordPort(int port) {
		this.chordPort = port;
	}
	
	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}
	
	public URL getUrl() {
		String url = "";
		try {
			InetAddress localhost = InetAddress.getLocalHost();
			url = this.protocol + "://" + localhost.getHostAddress() + ":" + chordPort + "/";
			return new URL(url);
		} catch (UnknownHostException e) {
			System.err.println("Failed to get address of localhost");
			e.printStackTrace();
		} catch (MalformedURLException e) {
			System.err.println("The URL created for this machine is not well formed [" + url + "]");
			e.printStackTrace();
		}
		return null;
	}
	
	public void setChordNode(ChordNode chordNode) {
		this.chordNode = chordNode;
	}
	
	public ChordNode getChordNode() {
		return this.chordNode;
	}

	public URL getBootstrapUrl() {
		return bootstrapUrl;
	}

	public void setBootstrapUrl(String bootstrapUrl) {
		try {
			System.out.println("tcp://" + bootstrapUrl + ":" + this.chordPort + "/");
			this.bootstrapUrl = new URL(URL.KNOWN_PROTOCOLS.get(URL.SOCKET_PROTOCOL).toString() + "://" + bootstrapUrl + ":" + this.chordPort + "/");
		} catch (MalformedURLException e) {
			System.err.println("Bootstrap URL is malformed");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public String getConfigDir() {
		return configDir;
	}

	public void setConfigDir(String configDir) {
		this.configDir = configDir;
	}

	public String getSlaveConfigFile() {
		return slaveConfigFile;
	}

	public void setSlaveConfigFile(String slaveConfigFile) {
		this.slaveConfigFile = slaveConfigFile;
	}

	public String getProtocol() {
		return protocol;
	}

	public int getMapReducePort() {
		return mapReducePort;
	}

	public int getChrodPort() {
		return chordPort;
	}
	
	public void setFirstNode(String firstNode) {
		if (firstNode != null && firstNode.equalsIgnoreCase("true")) {
			setFirstNode(true);
		} else {
			setFirstNode(false);
		}
	}
	
	public void setNodeType(String type) {
		if (type == null) {
			return;
		}
		if (type.equalsIgnoreCase("nameNode")) {
			this.masterType = MasterType.NAME_NODE;
			this.isMaster = true;
		} else if (type.equalsIgnoreCase("jobTracker")) {
			this.masterType = MasterType.JOB_TRACKER;
			this.isMaster = true;
		} else if (type.equalsIgnoreCase("dataNode")) {
			this.slaveType = SlaveType.DATA_NODE;
			this.isSlave = true;
		} else if (type.equalsIgnoreCase("taskTracker")) {
			this.slaveType = SlaveType.TASK_TRACKER;
			this.isSlave = true;
		}
	}
	
	public boolean isMaster() {
		return this.isMaster;
	}
	
	public boolean isSlave() {
		return this.isSlave;
	}
	
	public MasterType getMasterType() {
		return this.masterType;
	}
	
	public SlaveType getSlaveType() {
		return this.slaveType;
	}
	
	public String getBinDir() {
		return this.binDir;
	}
	
	public void setBinDir(String binDir) {
		this.binDir = binDir;
	}
}
