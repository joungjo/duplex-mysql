package com.geovis.duplex.mysql.handle;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.GtidEvent;
import com.google.code.or.binlog.impl.event.IncidentEvent;
import com.google.code.or.binlog.impl.event.IntvarEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RandEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.StopEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.UserVarEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.binlog.impl.event.XidEvent;

/**
 * 
 * @author jangzo
 *
 */
public interface Adaptor {
	
	public boolean match(long id);
	
	public void handleRowsEvent(AbstractRowEvent event);
	public void handleDeleteRowsEvent(DeleteRowsEventV2 event);
	public void handleWriteRowsEvent(WriteRowsEventV2 event);
	public void handleUpdateRowsEvent(UpdateRowsEventV2 event);
	public void handleTableMapEvent(TableMapEvent event);
	public void handleXidEvent(XidEvent event);
	public void handleQueryEvent(QueryEvent event);
	public void handleRotateEvent(RotateEvent event);
	public void handleFormatDescriptionEvent(FormatDescriptionEvent event);
	public void handleGtidEvent(GtidEvent event);
	public void handleIncidentEvent(IncidentEvent event);
	public void handleIntvarEvent(IntvarEvent event);
	public void handleRandEvent(RandEvent event);
	public void handleStopEvent(StopEvent event);
	public void handleUserVarEvent(UserVarEvent event);
	public void pass(BinlogEventV4 event);
	
	public String getName();
	
	/**
	 * status code indicates whether the Context has
	 * been updated when comparing the new Contexts.
	 * If the same, there is no updates. 
	 * @return status code
	 */
	public int getStatus();
	
}
