package com.geovis.duplex.mysql.execute;

import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;

public interface Executor {
	
	void insert(WriteRowsEventV2 event);
	
	public void update(UpdateRowsEventV2 event);
	
	public void delete(DeleteRowsEventV2 event);
	
	public String getName();

}
