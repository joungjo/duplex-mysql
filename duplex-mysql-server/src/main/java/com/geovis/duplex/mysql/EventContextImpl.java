package com.geovis.duplex.mysql;

import java.io.Serializable;

import com.geovis.duplex.jms.Pusher;
import com.geovis.duplex.utils.Utils;
import com.google.code.or.binlog.BinlogParseRecord;
import com.google.code.or.binlog.EventContext;
import com.google.code.or.binlog.impl.event.TableMapEvent;

public class EventContextImpl implements EventContext {
	private TableMapEvent event;
	private Pusher pusher;
	
	public EventContextImpl(Pusher pusher) {
		this.pusher = pusher;
		TableMapEvent event = BinlogParseRecord.single.readTableMapEvent();
		if (event != null) {
			event.setFieldMetas(Utils.getMetadata(event));
			this.event = event;
		}
	}

	@Override
	public Object get() {
		return event;
	}

	@Override
	public void set(Object object) {
		this.event = (TableMapEvent)object;
		if (event.getFieldMetas() == null) {
			event.setFieldMetas(Utils.getMetadata(event));
		}
	}

	@Override
	public void doSomething(Serializable o) {
		pusher.push(o);
	}

}
