package com.geovis.duplex.jms;

import java.io.Serializable;

public interface Pusher {
	
	public void push(Serializable o);

	public void close();

	public void build();
}
