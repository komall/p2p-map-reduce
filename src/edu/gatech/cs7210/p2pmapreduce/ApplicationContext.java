package edu.gatech.cs7210.p2pmapreduce;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;

import de.uniba.wiai.lspi.chord.data.URL;

public class ApplicationContext {
	
	private boolean firstNode = false;
	private String protocol = "http";
	private int port = 3333;

	private ApplicationContext() { }
	
	private static ApplicationContext INSTANCE = new ApplicationContext();
	
	public static ApplicationContext getInstance() {
		return INSTANCE;
	}
	
	public boolean isFirstNode() {
		return firstNode;
	}
	
	public void setFirstNode(boolean firstNode) {
		this.firstNode = firstNode;
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	
	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}
	
	public URL getUrl() {
		String url = "";
		try {
			InetAddress localhost = InetAddress.getLocalHost();
			url = this.protocol + "://" + localhost.getHostAddress() + ":" + port + "/";
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
}
