package com.geovis.duplex.mysql.handle;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;

import org.apache.log4j.Logger;

import com.geovis.duplex.jms.MessageHandle;
import com.geovis.duplex.mysql.driver.ConnectionFactory;
import com.geovis.duplex.mysql.execute.Executor;
import com.geovis.duplex.mysql.execute.MappedTableExecutor;
import com.geovis.duplex.mysql.model.NodeModel;
import com.geovis.duplex.mysql.model.TableModel;
import com.geovis.duplex.mysql.receive.MysqlReceive;
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

public class MysqlHandle implements MessageHandle, Adaptor {
	private static final Logger logger = Logger.getLogger(MysqlHandle.class);
	private Connection connection;
	private NodeModel node;
	private boolean allDefaults = false;
	private Map<String, Boolean> databaseDefaults = new HashMap<>();
	private Map<String, Executor> executors = new HashMap<>();
	private MysqlReceive mysqlReceive;
	private Statement statement;
	{
		try {
			connection = ConnectionFactory.getConnection();
			connection.setAutoCommit(false);
			statement = connection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public MysqlHandle() {
	}

	public MysqlHandle(NodeModel node) {
		this.node = node;
		init();
	}

	public MysqlHandle(MysqlReceive mysqlReceive, NodeModel node) {
		this.node = node;
		this.mysqlReceive = mysqlReceive;
		init();
	}

	private void init() {
		Map<String, List<TableModel>> schemas = node.getSchemas();
		allDefaults = schemas == null || schemas.isEmpty();
		if (!allDefaults) {
			for (String database : schemas.keySet()) {
				List<TableModel> list = schemas.get(database);
				boolean isDefault = list == null || list.isEmpty();
				databaseDefaults.put(database, isDefault);
				if (!isDefault) {
					for (TableModel tableModel : list) {
						String key = database + "." + tableModel.getName();
						executors.put(key , new MappedTableExecutor(connection, tableModel));
					}
				}
			}
		}
	}

	@Override
	public void handle(Message message) throws JMSException {
		Object object = ((ObjectMessage)message).getObject();
		if (object instanceof BinlogEventV4) {
			BinlogEventV4 event = (BinlogEventV4)object;
			EventHandleFactory.getEventHandle(
					event.getHeader().getEventType()).handle(event, this);
		}
	}

	@Override
	public void commit() {
		try {
			connection.commit();
			mysqlReceive.commit();
		} catch (Exception e) {
			rollback();
			logger.error(e);
			e.printStackTrace();
		}
	}

	@Override
	public void rollback() {
		try {
			mysqlReceive.rollback();
			connection.rollback();
		} catch (SQLException e) {
			logger.error(e);
			e.printStackTrace();
		}
	}

	@Override
	public boolean match(long id) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void handleRowsEvent(AbstractRowEvent event) {

	}

	@Override
	public void handleDeleteRowsEvent(DeleteRowsEventV2 event) {
		Executor executor = getExecutor(event);
		if (executor != null) {
			executor.delete(event);
		}
	}

	@Override
	public void handleWriteRowsEvent(WriteRowsEventV2 event) {
		Executor executor = getExecutor(event);
		if (executor != null) {
			executor.insert(event);
		}
	}

	@Override
	public void handleUpdateRowsEvent(UpdateRowsEventV2 event) {
		Executor executor = getExecutor(event);
		if (executor != null) {
			executor.update(event);
		}
	}

	private Executor getExecutor(AbstractRowEvent event) {
		String database = event.getDatabaseName();
		String table = event.getTableName();
		String key = database + "." + table;
		Executor executor = executors.get(key);
		if (executor == null) {
			if (allDefaults || databaseDefaults.get(database)) {
				executor = new MappedTableExecutor(connection, database, table);
				executors.put(key, executor);
			}
		}
		return executor;
	}

	@Override
	public void handleTableMapEvent(TableMapEvent event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleXidEvent(XidEvent event) {
		commit();
	}

	@Override
	public void handleQueryEvent(QueryEvent event) {
		if (event.getHeader().getFlags() == 0) {
			try {
				String sql = event.getSql().toString();
				String database = event.getDatabaseName().toString();
				try {
					statement.execute("use " + database);
				} catch (SQLException e) {
					if (e.getErrorCode() != 1050)
						throw e;
				}
				statement.execute(sql);
				connection.commit();
			} catch (SQLException e) {
				switch (e.getErrorCode()) {
				case 1049: //Unknown table
				case 1050: //Table already exists
				case 1051: //Unknown database 
				case 1007: //database exists
					return;
				default:
					logger.error(e);
					e.printStackTrace();
				}
			} finally {
				try {
					statement.execute("use " + ConnectionFactory.getIgnore());
				} catch (SQLException e) {
					logger.error(e);
					e.printStackTrace();
				}
				commit();
			}
		}
	}

	@Override
	public void handleRotateEvent(RotateEvent event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleFormatDescriptionEvent(FormatDescriptionEvent event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleGtidEvent(GtidEvent event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleIncidentEvent(IncidentEvent event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleIntvarEvent(IntvarEvent event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleRandEvent(RandEvent event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleStopEvent(StopEvent event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleUserVarEvent(UserVarEvent event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void pass(BinlogEventV4 event) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getStatus() {
		// TODO Auto-generated method stub
		return 0;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
		try {
			this.connection.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}