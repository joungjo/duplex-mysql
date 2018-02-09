package com.geovis.duplex.mysql.handle.impl;

import com.geovis.duplex.mysql.handle.Adaptor;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;

public class DeleteRowsEventHandle extends DefaultEventHandle {

	@Override
	public void handle(BinlogEventV4 event, Adaptor adaptor) {
		DeleteRowsEventV2 rowsEvent = (DeleteRowsEventV2)event;
		long tableId = rowsEvent.getTableId();
		if (adaptor.match(tableId)) {
			adaptor.handleDeleteRowsEvent(rowsEvent);
		}
	}

}
