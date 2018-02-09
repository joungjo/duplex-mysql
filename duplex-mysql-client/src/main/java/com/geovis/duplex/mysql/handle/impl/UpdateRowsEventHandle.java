package com.geovis.duplex.mysql.handle.impl;

import com.geovis.duplex.mysql.handle.Adaptor;
import com.geovis.duplex.mysql.handle.EventHandle;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;

public class UpdateRowsEventHandle implements EventHandle {

	@Override
	public void handle(final BinlogEventV4 event, Adaptor context) {
		UpdateRowsEventV2 rowsEvent = ((UpdateRowsEventV2) event);
		long tableId = rowsEvent.getTableId();
		if (context.match(tableId)) {
			context.handleUpdateRowsEvent(rowsEvent);
		}
	}

}
