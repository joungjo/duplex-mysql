package com.geovis.duplex.mysql.handle;

import com.google.code.or.binlog.BinlogEventV4;

/**
 * 
 * @author jangzo
 *
 */
public interface EventHandle {
	
	
	/**
	 * Handle the subjects of BinlogEventV4, and call Adaptor 
	 * methods 
	 * @param event
	 */
	public void handle(BinlogEventV4 event, Adaptor adaptor);
	
}
