package com.geovis.duplex.mysql.handle.impl;

import com.geovis.duplex.mysql.handle.Adaptor;
import com.geovis.duplex.mysql.handle.EventHandle;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.RandEvent;

public class RandEventHandle implements EventHandle {

	@Override
	public void handle(BinlogEventV4 event, Adaptor adaptor) {
		adaptor.handleRandEvent((RandEvent) event);
	}

}
