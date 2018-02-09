package com.geovis.duplex.mysql;

import java.util.concurrent.atomic.AtomicBoolean;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.BinlogParseRecord;
import com.google.code.or.binlog.EventContext;
import com.google.code.or.binlog.impl.event.BinlogEventV4HeaderImpl;
import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
/**
 * Abundant the replication events, keep binlog pos 
 * and keep filter status
 * @version 1.0.0
 * @author jangzo 20161005
 */
public class RowFormatFilter implements BinlogEventListener {
	private String ignoreDatabase;
	private EventContext context;
	//false : replication start, true : replication end
	private final AtomicBoolean sign = new AtomicBoolean(true);

	{
		byte flag = BinlogParseRecord.positionReadByte();
		if (flag == -1) {
			sign.set(BinlogParseRecord.positionReadByte(1) == -1);
		}
	}

	public RowFormatFilter() { 
	}

	public RowFormatFilter(String ignoreDatabase) {
		this.ignoreDatabase = ignoreDatabase;
	}

	public RowFormatFilter(String ignoreDatabase, EventContext context) {
		this.ignoreDatabase = ignoreDatabase;
		this.context = context;
	}

	@Override
	public void onEvents(BinlogEventV4 event) {
		BinlogEventV4Header header = event.getHeader();

		/*if (header.getEventType() == QueryEvent.EVENT_TYPE) {
			QueryEvent queryEvent = (QueryEvent) event;
			String database = queryEvent.getDatabaseName().toString();
			if (database.equals(ignoreDatabase)) {
				sign.compareAndSet(true, false);
			}
		} else if (header.getEventType() == XidEvent.EVENT_TYPE
				&& sign.compareAndSet(false, true)) {
			skip.set(true);
			
		} else if (header.getEventType() == TableMapEvent.EVENT_TYPE) {
			BinlogParseRecord.tableMapWrite(((TableMapEvent)event).copy());
		} 
*/

		switch (header.getEventType()) {
		case QueryEvent.EVENT_TYPE:
			QueryEvent queryEvent = (QueryEvent) event;
			String database = queryEvent.getDatabaseName().toString();
			if (ignoreDatabase.equals(database)) {
				sign.compareAndSet(true, false);
			}
			break;
		case XidEvent.EVENT_TYPE:
			if (sign.compareAndSet(false, true)) {
				BinlogParseRecord.writeHeader(header, sign);
				return;
			}
			break;
		case  TableMapEvent.EVENT_TYPE:
			BinlogParseRecord.tableMapWrite(((TableMapEvent)event).copy());
			break;
		case FormatDescriptionEvent.EVENT_TYPE:
			return;
		case RotateEvent.EVENT_TYPE : 
			RotateEvent rotateEvent = (RotateEvent)event;
			BinlogEventV4HeaderImpl headerImpl = new BinlogEventV4HeaderImpl();
			headerImpl.setBinlogFileName(rotateEvent.getBinlogFileName().toString());
			headerImpl.setNextPosition(rotateEvent.getBinlogPosition());
			headerImpl.setEventLength(0);
			BinlogParseRecord.writeHeader(headerImpl, sign);
			return;
		default:
			break;
		}

		if (sign.get()) { 
			event.callback(context);
			System.out.println(event);
		}
		BinlogParseRecord.writeHeader(header, sign);

	}

	public String getIgnoreDatabase() {
		return ignoreDatabase;
	}

	public void setIgnoreDatabase(String ignoreDatabase) {
		this.ignoreDatabase = ignoreDatabase;
	}

	public EventContext getContext() {
		return context;
	}

	public void setContext(EventContext context) {
		this.context = context;
	}
}
