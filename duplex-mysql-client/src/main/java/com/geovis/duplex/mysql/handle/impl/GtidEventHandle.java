package com.geovis.duplex.mysql.handle.impl;

import com.geovis.duplex.mysql.handle.Adaptor;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.GtidEvent;

public class GtidEventHandle extends DefaultEventHandle {

	@Override
	public void handle(BinlogEventV4 event, Adaptor adaptor) {
		adaptor.handleGtidEvent((GtidEvent) event);
	}

}
