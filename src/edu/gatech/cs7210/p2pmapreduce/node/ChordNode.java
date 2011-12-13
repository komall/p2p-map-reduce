package edu.gatech.cs7210.p2pmapreduce.node;

import java.net.MalformedURLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import de.uniba.wiai.lspi.chord.data.URL;
import de.uniba.wiai.lspi.chord.service.Chord;
import de.uniba.wiai.lspi.chord.service.PropertiesLoader;
import de.uniba.wiai.lspi.chord.service.ServiceException;
import de.uniba.wiai.lspi.chord.service.impl.ChordImpl;
import edu.gatech.cs7210.p2pmapreduce.ApplicationContext;

public class ChordNode {
	
	private Chord chord = new ChordImpl();
	
	public void runAsFirst() {
		try {
			chord.create(ApplicationContext.getInstance().getUrl());
		} catch (ServiceException e) {
			System.err.println("Could not create Chord service");
			e.printStackTrace();
		}
	}
	
	public void run(String bootstrapUrl) {
		try {
			chord.join(ApplicationContext.getInstance().getUrl(), new URL(bootstrapUrl));
		} catch (ServiceException e) {
			System.err.println("Could not create Chord service");
			e.printStackTrace();
		} catch (MalformedURLException e) {
			System.err.println("Bootstrap URL is not well formed");
			e.printStackTrace();
		}
	}
	
	public void startHadoop() {
		
	}

	public static void main(String[] args) {
		
		PropertiesLoader.loadPropertyFile();
		ApplicationContext.getInstance().setProtocol(URL.KNOWN_PROTOCOLS.get(URL.SOCKET_PROTOCOL));
		
		// create Options object
		Options options = new Options();

		// add options
		options.addOption("firstNode", true, "specify if this is the first node in the chord topology");
		options.addOption("bootstrapUrl", true, "if not the first node, specify the url of the first node");
		options.addOption("port", true, "the port on which this chord node should listen");
		options.addOption("nameNode", true, "is this a NameNode");
		options.addOption("jobTracker", true, "is this a JobTracker");
		options.addOption("dataNode", true, "is this a DataNode");
		options.addOption("taskTracker", true, "is this a TaskTracker");
		
		// parse the options
		CommandLineParser parser = new PosixParser();
		try {
			CommandLine cmd = parser.parse(options, args);
			
			ChordNode node = new ChordNode();
			
			String firstNode = cmd.getOptionValue("firstNode");
			String bootstrapUrl = cmd.getOptionValue("bootstrapUrl");
			String port = cmd.getOptionValue("port");
			
			if (firstNode == null && bootstrapUrl == null) {
				System.err.println("One of firstNode or bootstrap Url must be specified");
				System.exit(-1);
			}
			
			if (port == null) {
				System.err.println("Must specifiy port");
				System.exit(-1);
			} else {
				ApplicationContext.getInstance().setPort(Integer.parseInt(port));
			}
			
			if (firstNode != null) {
				ApplicationContext.getInstance().setFirstNode(true);
				System.out.println("Starting ChordNode as first node in chord topology");
				node.runAsFirst();
			} else {
				node.run(bootstrapUrl);
			}
		} catch (Exception e) {
			System.err.println("Failed to parse arugments");
		}
	}
}
