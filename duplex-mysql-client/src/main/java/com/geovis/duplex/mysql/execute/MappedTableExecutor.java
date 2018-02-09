package com.geovis.duplex.mysql.execute;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.geovis.duplex.mysql.model.TableModel;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.FieldMeta;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;

public class MappedTableExecutor implements Executor {
	private static final Logger logger = Logger.getLogger(MappedTableExecutor.class);

	private String database;
	private String table;

	private Connection connection;
	private PreparedStatement insert;
	private PreparedStatement delete;
	private PreparedStatement update;

	private boolean hasKey = false;

	private Map<String, Integer> orderMap = new HashMap<>();
	private Map<String, Integer> keyMap = new HashMap<>();

	private List<FieldMeta> fieldMetas;
	private byte[] types;
	private int size;
	private boolean isFirst = true;

	public MappedTableExecutor(Connection connection, TableModel tableModel) {
		this.connection = connection;

		if (tableModel.getLocalSchema() == null) {
			this.database = tableModel.getSchema();
		} else {
			this.database = tableModel.getLocalSchema();
		}
		if (tableModel.getLocalTable() == null) {
			this.table = tableModel.getName();
		} else {
			this.table = tableModel.getLocalTable();
		}

		Map<String, String> fieldPairs = tableModel.getFieldPairs();
		init(fieldPairs);
	}

	public MappedTableExecutor(Connection connection, String database, String table) {
		this.database = database;
		this.table = table;
		this.connection = connection;
		init(null);
	}

	private void init(Map<String, String> fieldPairs) {
		StringBuilder iSql = new StringBuilder("insert into ");
		StringBuilder dSql = new StringBuilder("delete from ");
		StringBuilder uSql = new StringBuilder("update ");
		iSql.append(database).append(".").append(table).append("(");
		dSql.append(database).append(".").append(table).append(" where 1=1");
		uSql.append(database).append(".").append(table).append(" set ");

		StringBuilder col = new StringBuilder();
		StringBuilder con = new StringBuilder();
		StringBuilder val = new StringBuilder();
		StringBuilder key = new StringBuilder();
		StringBuilder nk  = new StringBuilder();

		try {
			DatabaseMetaData metaData = connection.getMetaData();
			ResultSet columns = metaData.getColumns(database, database, table, null);
			int i = 1;
			if (fieldPairs != null) {
				while(columns.next()){
					String columnName = columns.getString(4);
					if (fieldPairs.containsKey(columnName)) {
						orderMap.put(fieldPairs.get(columnName), i++);
						nk.append(" and ").append(columnName).append("=?");
						col.append(columnName).append(",");
						con.append(columnName).append("=?,");
						val.append("?,");
					}
				}
			} else {
				while(columns.next()){
					String columnName = columns.getString(4);
					orderMap.put(columnName, i++);
					nk.append(" and ").append(columnName).append("=?");
					col.append(columnName).append(",");
					con.append(columnName).append("=?,");
					val.append("?,");
				}
			}
			columns.close();
			ResultSet primaryKeys = metaData.getPrimaryKeys(database, database, table);

			while(primaryKeys.next()){
				String columnName = primaryKeys.getString(4);

				key.append(" and ").append(columnName).append("=?");
				hasKey = true;
				if (fieldPairs != null) {
					if (fieldPairs.containsKey(columnName)) {
						keyMap.put(fieldPairs.get(columnName), i++);
					}
				} else {
					keyMap.put(columnName, i++);
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		con.deleteCharAt(con.lastIndexOf(","));
		val.deleteCharAt(val.lastIndexOf(","));
		col.deleteCharAt(col.lastIndexOf(","));

		iSql.append(col).append(")values(").append(val).append(")");
		uSql.append(con);
		if (hasKey) {
			dSql.append(key);
			uSql.append(" where 1=1").append(key);
		} else {
			dSql.append(nk);
			uSql.append(" where 1=1").append(nk);
		}
		System.out.println(iSql);
		System.out.println(uSql);
		System.out.println(dSql);


		try {
			insert = connection.prepareStatement(iSql.toString());
			update = connection.prepareStatement(uSql.toString());
			delete = connection.prepareStatement(dSql.toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void insert(WriteRowsEventV2 event) {
		if (isFirst) {
			firstRowEvent(event);
		}
		List<Row> data = event.getRows();
		try {
			for (Row row : data) {
				List<Column> columns = row.getColumns();
				for (int i = 0; i < size; i++) {
					FieldMeta meta = fieldMetas.get(i);
					String columnName = meta.getColumnName();
					Integer integer = orderMap.get(columnName);
					if (integer != null) {
						setValue(insert, columns.get(i).getValue(), integer, types[i]);
					}
				}
				insert.addBatch();
			}
			insert.executeBatch();
		} catch (SQLException e1) {
			logger.error(database);
			logger.error(table);
			logger.error(fieldMetas);
			logger.error(data);
			logger.error(e1);
		}

	}

	@Override
	public void update(UpdateRowsEventV2 event) {
		if (isFirst) {
			firstRowEvent(event);
		}
		List<Pair<Row>> data = event.getRows();
		if (hasKey) {
			try {
				for (Pair<Row> pair : data) {
					Row after = pair.getAfter();
					List<Column> columns = after.getColumns();

					for (int i = 0; i < size; i++) {
						String columnName = fieldMetas.get(i).getColumnName();
						Integer integer = orderMap.get(columnName);
						if (integer != null) {
							setValue(update, columns.get(i).getValue(), integer, types[i]);
						}
						Integer key = keyMap.get(columnName);
						if (key != null) {
							setValue(update, columns.get(i).getValue(), key, types[i]);
						}

					}
					update.addBatch();
				}
				update.executeBatch();
			} catch (SQLException e) {
				logger.error(data);
				logger.error(e);
			}
			return;
		}
	}

	@Override
	public void delete(DeleteRowsEventV2 event) {
		if (isFirst) {
			firstRowEvent(event);
		}
		List<Row> data = event.getRows();
		int j = orderMap.size();
		if (hasKey) {
			try {
				for (Row row : data) {
					List<Column> columns = row.getColumns();
					for (int i = 0; i < size; i++) {
						FieldMeta meta = fieldMetas.get(i);
						String columnName = meta.getColumnName();
						Integer key = keyMap.get(columnName);
						if (key != null) {
							setValue(delete, columns.get(i).getValue(), key - j, types[i]);
						}
					}

					delete.addBatch();
				}
				delete.executeBatch();
			} catch (SQLException e) {
				logger.error(data);
				logger.error(e);
			}
			return;
		}

	}

	@Override
	public String getName() {
		return database + "." + table;
	}

	/**
		<p/>Protocol::MYSQL_TYPE_DECIMAL 		0x00 	Implemented by ProtocolBinary::MYSQL_TYPE_DECIMAL
		<p/>Protocol::MYSQL_TYPE_TINY 			0x01 	Implemented by ProtocolBinary::MYSQL_TYPE_TINY
		<p/>Protocol::MYSQL_TYPE_SHORT 			0x02 	Implemented by ProtocolBinary::MYSQL_TYPE_SHORT
		<p/>Protocol::MYSQL_TYPE_LONG 			0x03 	Implemented by ProtocolBinary::MYSQL_TYPE_LONG
		<p/>Protocol::MYSQL_TYPE_FLOAT 			0x04 	Implemented by ProtocolBinary::MYSQL_TYPE_FLOAT
		<p/>Protocol::MYSQL_TYPE_DOUBLE 		0x05 	Implemented by ProtocolBinary::MYSQL_TYPE_DOUBLE
		<p/>Protocol::MYSQL_TYPE_NULL 			0x06 	Implemented by ProtocolBinary::MYSQL_TYPE_NULL
		<p/>Protocol::MYSQL_TYPE_TIMESTAMP 		0x07 	Implemented by ProtocolBinary::MYSQL_TYPE_TIMESTAMP
		<p/>Protocol::MYSQL_TYPE_LONGLONG 		0x08 	Implemented by ProtocolBinary::MYSQL_TYPE_LONGLONG
		<p/>Protocol::MYSQL_TYPE_INT24 			0x09 	Implemented by ProtocolBinary::MYSQL_TYPE_INT24
		<p/>Protocol::MYSQL_TYPE_DATE 			0x0a 	Implemented by ProtocolBinary::MYSQL_TYPE_DATE
		<p/>Protocol::MYSQL_TYPE_TIME 			0x0b 	Implemented by ProtocolBinary::MYSQL_TYPE_TIME
		<p/>Protocol::MYSQL_TYPE_DATETIME 		0x0c 	Implemented by ProtocolBinary::MYSQL_TYPE_DATETIME
		<p/>Protocol::MYSQL_TYPE_YEAR 			0x0d 	Implemented by ProtocolBinary::MYSQL_TYPE_YEAR
		<p/>Protocol::MYSQL_TYPE_NEWDATE [a] 	0x0e 	see Protocol::MYSQL_TYPE_DATE
		<p/>Protocol::MYSQL_TYPE_VARCHAR 		0x0f 	Implemented by ProtocolBinary::MYSQL_TYPE_VARCHAR
		<p/>Protocol::MYSQL_TYPE_BIT 			0x10 	Implemented by ProtocolBinary::MYSQL_TYPE_BIT
		<p/>Protocol::MYSQL_TYPE_TIMESTAMP2 [a] 0x11 	see Protocol::MYSQL_TYPE_TIMESTAMP
		<p/>Protocol::MYSQL_TYPE_DATETIME2 [a] 	0x12 	see Protocol::MYSQL_TYPE_DATETIME
		<p/>Protocol::MYSQL_TYPE_TIME2 [a] 		0x13 	see Protocol::MYSQL_TYPE_TIME
		<p/>Protocol::MYSQL_TYPE_NEWDECIMAL 	0xf6 	Implemented by ProtocolBinary::MYSQL_TYPE_NEWDECIMAL
		<p/>Protocol::MYSQL_TYPE_ENUM 			0xf7 	Implemented by ProtocolBinary::MYSQL_TYPE_ENUM
		<p/>Protocol::MYSQL_TYPE_SET 			0xf8 	Implemented by ProtocolBinary::MYSQL_TYPE_SET
		<p/>Protocol::MYSQL_TYPE_TINY_BLOB 		0xf9 	Implemented by ProtocolBinary::MYSQL_TYPE_TINY_BLOB
		<p/>Protocol::MYSQL_TYPE_MEDIUM_BLOB 	0xfa 	Implemented by ProtocolBinary::MYSQL_TYPE_MEDIUM_BLOB
		<p/>Protocol::MYSQL_TYPE_LONG_BLOB 		0xfb 	Implemented by ProtocolBinary::MYSQL_TYPE_LONG_BLOB
		<p/>Protocol::MYSQL_TYPE_BLOB 			0xfc 	Implemented by ProtocolBinary::MYSQL_TYPE_BLOB
		<p/>Protocol::MYSQL_TYPE_VAR_STRING 	0xfd 	Implemented by ProtocolBinary::MYSQL_TYPE_VAR_STRING
		<p/>Protocol::MYSQL_TYPE_STRING 		0xfe 	Implemented by ProtocolBinary::MYSQL_TYPE_STRING
		<p/>Protocol::MYSQL_TYPE_GEOMETRY 		0xff 	 
	 * @param pst
	 * @param value
	 * @param i column index
	 * @param type type code
	 * @throws SQLException 
	 */
	private void setValue(PreparedStatement pst, Object value, int i, byte type) throws SQLException {
		// TODO Auto-generated method stub
		switch (type) {

		case 0x00:
		case (byte) 0xf6:
			pst.setBigDecimal(i, (BigDecimal)value);
		break;

		case 0x0b:
		case 0x13:
			pst.setTime(i, (Time)value);
			break;

		case 0x0a:
		case 0x0c:
		case 0x12:
			pst.setDate(i, (Date)value);
			break;

		case 0x07:
		case 0x11:
			pst.setTimestamp(i, (Timestamp)value);
			break;

		case 0x10:
		case (byte) 0xf9:
		case (byte) 0xfa:
		case (byte) 0xfb:
		case (byte) 0xfc:
		case (byte) 0xfd:
		case (byte) 0xfe:
		case (byte) 0xff:
			pst.setBytes(i, (byte[]) value);
		break;

		case 0x0d:
		case 0x0e:
		case 0x0f:
		case (byte) 0xf7:
		case (byte) 0xf8:
		case 0x01:
		case 0x02:
		case 0x03:
		case 0x04:
		case 0x05:
		case 0x06:
		case 0x08:
		case 0x09:
		default:
			pst.setObject(i, value);
			break;
		}
	}
	/*
	 * 
	 */
	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public PreparedStatement getInsert() {
		return insert;
	}

	public void setInsert(PreparedStatement insert) {
		this.insert = insert;
	}

	public PreparedStatement getDelete() {
		return delete;
	}

	public void setDelete(PreparedStatement delete) {
		this.delete = delete;
	}

	public PreparedStatement getUpdate() {
		return update;
	}

	public void setUpdate(PreparedStatement update) {
		this.update = update;
	}

	public void firstRowEvent(AbstractRowEvent event){
		fieldMetas = event.getFieldMetas();
		types = event.getTypes();
		size = types.length;

	}

}
