package com.geovis.duplex.mysql.handle.impl;

import com.geovis.duplex.mysql.handle.Adaptor;
import com.geovis.duplex.mysql.handle.EventHandle;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;

public class WriteRowsEventHandle implements EventHandle {

	@Override
	public void handle(BinlogEventV4 event, Adaptor adaptor) {
		WriteRowsEventV2 rowsEvent = (WriteRowsEventV2)event;
		long tableId = rowsEvent.getTableId();
		if (adaptor.match(tableId)) {
			adaptor.handleWriteRowsEvent(rowsEvent);
		}
	}

}
