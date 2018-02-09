package com.geovis.duplex.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.code.or.binlog.impl.event.FieldMeta;
import com.google.code.or.binlog.impl.event.TableMapEvent;

public class Utils {
	private static Map<String, List<FieldMeta>> fieldMetas = new HashMap<>();
	private static Properties prop = new Properties();
	static {
		InputStream in = null;
		try {
			File f = new File(System.getProperty("user.dir") + "/conf/master.properties");
			in = new FileInputStream(f);
			prop.load(in);
			in.close();
			Class.forName(prop.getProperty("mysql.driver"));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static Connection getMysqlConnection() 
			throws SQLException {
			StringBuilder sb = new StringBuilder("jdbc:mysql://");
			sb.append(getProperty("mysql.ip")).append(":")
			.append(getProperty("mysql.port"))
			.append("/").append(getProperty("mysql.ignore.database"));
			return DriverManager.getConnection(sb.toString(), 
					getProperty("mysql.user"), getProperty("mysql.password"));
	}
	
	public static String getProperty(String key) {
		return prop.getProperty(key);
	}
	
	public static List<FieldMeta> getMetadata(String database, String table) {
		if (fieldMetas.containsKey(database + "." + table)) {
			return fieldMetas.get(database + "." + table);
		}
		List<FieldMeta> list = null;
		try {
			Connection connection = getMysqlConnection();
			Statement statement = connection.createStatement();
			ResultSet rs = statement.executeQuery("desc " + database + "." + table);
			list = new ArrayList<FieldMeta>();
			while(rs.next()) {
				FieldMeta fieldMeta = new FieldMeta();
				fieldMeta.setColumnName(rs.getString("Field"));
				fieldMeta.setColumnType(rs.getString("Type"));
				fieldMeta.setDefaultValue(rs.getString("Default"));
				fieldMeta.setExtra(rs.getString("Extra"));
				fieldMeta.setIskey(rs.getString("Key"));
				fieldMeta.setIsNullable(rs.getString("Null"));
				list.add(fieldMeta);
			}
			fieldMetas.put(database + "." + table, list);
			rs.close();
			statement.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public static List<FieldMeta> getMetadata(TableMapEvent event) {
		return getMetadata(event.getDatabaseName().toString(), event.getTableName().toString());
	}
}
