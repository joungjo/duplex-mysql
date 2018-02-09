package com.geovis.duplex.util;

public abstract class AbstractStatusRunnable implements StatusRunnable {
	protected boolean status = true;
	protected volatile long count = 0;

	@Override
	public void stop(){
		this.status = false;
	}

	@Override
	public void start(){
		this.status = true;
	}
	
	public void count(){
		count++;
	}

	public boolean isStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}
	
}
