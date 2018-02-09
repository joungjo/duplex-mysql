package com.geovis.duplex.mysql.handle.impl;

import com.geovis.duplex.mysql.handle.Adaptor;
import com.geovis.duplex.mysql.handle.EventHandle;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.UserVarEvent;

public class UserVarEventHandle implements EventHandle {

	@Override
	public void handle(BinlogEventV4 event, Adaptor adaptor) {
		adaptor.handleUserVarEvent((UserVarEvent) event);
	}

}
