package edu.gatech.cs7210.p2pmapreduce.chord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import de.uniba.wiai.lspi.chord.data.URL;
import de.uniba.wiai.lspi.chord.service.Key;

public class UrlKey implements Key {

	private URL url;
	
	public UrlKey(URL url) {
		this.url = url;
	}
	public byte [] getBytes() {
		byte[] bytes = null;
		  ByteArrayOutputStream bos = new ByteArrayOutputStream();
		  try {
		    ObjectOutputStream oos = new ObjectOutputStream(bos);
		    oos.writeObject(this.url);
		    oos.flush(); 
		    oos.close(); 
		    bos.close();
		    bytes = bos.toByteArray ();
		  }
		  catch (IOException ex) {
		    //TODO: Handle the exception
		  }
		  return bytes;
	}
	
	public int hashCode () {
		return this.url.hashCode();
	}
	
	public boolean equals(Object o) {
		if ( o instanceof UrlKey ) {
			return ((UrlKey)o).url.equals(this.url);
		}
		return false;
	}
}
