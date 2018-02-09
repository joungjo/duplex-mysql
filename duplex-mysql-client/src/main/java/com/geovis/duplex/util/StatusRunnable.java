package com.geovis.duplex.util;

public interface StatusRunnable extends Runnable {
	
	public void stop();
	
	public void start();
	
	public long getCount();
}
