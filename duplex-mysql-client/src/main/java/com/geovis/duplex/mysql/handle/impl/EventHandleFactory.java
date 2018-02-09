package com.geovis.duplex.mysql.handle.impl;

import com.geovis.duplex.mysql.handle.EventHandle;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.IncidentEvent;
import com.google.code.or.binlog.impl.event.IntvarEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RandEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.StopEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.UserVarEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.binlog.impl.event.XidEvent;

public class EventHandleFactory {
	private final static EventHandle[] eventHandles = new EventHandle[104];
	private static EventHandle defaultHandle = new DefaultEventHandle();
	
	static {
		//register EventHandles
		eventHandles[DeleteRowsEvent.EVENT_TYPE] = new DeleteRowsEventHandle();
		eventHandles[DeleteRowsEventV2.EVENT_TYPE] = new DeleteRowsEventHandle();
		eventHandles[WriteRowsEvent.EVENT_TYPE] = new WriteRowsEventHandle();
		eventHandles[WriteRowsEventV2.EVENT_TYPE] = new WriteRowsEventHandle();
		eventHandles[UpdateRowsEvent.EVENT_TYPE] = new UpdateRowsEventHandle();
		eventHandles[UpdateRowsEventV2.EVENT_TYPE] = new UpdateRowsEventHandle();
		eventHandles[TableMapEvent.EVENT_TYPE] = new TableMapEventHandle();
		eventHandles[XidEvent.EVENT_TYPE] = new XidEventHandle();
		eventHandles[QueryEvent.EVENT_TYPE] = new QueryEventHandle();
		eventHandles[RotateEvent.EVENT_TYPE] = new RotateEventHandle();
		eventHandles[FormatDescriptionEvent.EVENT_TYPE] = new FormatDescriptionEventHandle();
		eventHandles[IncidentEvent.EVENT_TYPE] = new IncidentEventHandle();
		eventHandles[IntvarEvent.EVENT_TYPE] = new IntvarEventHandle();
		eventHandles[RandEvent.EVENT_TYPE] = new RandEventHandle();
		eventHandles[StopEvent.EVENT_TYPE] = new StopEventHandle();
		eventHandles[UserVarEvent.EVENT_TYPE] = new UserVarEventHandle();
	}
	public static EventHandle getEventHandle(int eventType) { 
		if (eventHandles[eventType] == null) {
			return defaultHandle;
		}
		return eventHandles[eventType];
	}
}
