package com.geovis.duplex.mysql.handle.impl;

import com.geovis.duplex.mysql.handle.Adaptor;
import com.geovis.duplex.mysql.handle.EventHandle;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.TableMapEvent;

public class TableMapEventHandle implements EventHandle  {
	@Override
	public void handle(BinlogEventV4 event, Adaptor adaptor) {
		TableMapEvent event2 = (TableMapEvent) event;
		adaptor.handleTableMapEvent(event2);
	}
	
}
