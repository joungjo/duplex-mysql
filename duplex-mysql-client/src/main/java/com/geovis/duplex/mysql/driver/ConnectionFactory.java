package com.geovis.duplex.mysql.driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {
	private static String url;
	private static String user;
	private static String password;
	private static String ignore;

	public static Connection getConnection() throws SQLException {
		return DriverManager.getConnection(url, user, password);
	}

	public static String getUrl() {
		return url;
	}

	public static void setUrl(String url) {
		ConnectionFactory.url = url;
	}

	public static String getUser() {
		return user;
	}

	public static void setUser(String user) {
		ConnectionFactory.user = user;
	}

	public static String getPassword() {
		return password;
	}

	public static void setPassword(String password) {
		ConnectionFactory.password = password;
	}

	public static String getIgnore() {
		return ignore;
	}

	public static void setIgnore(String ignore) {
		ConnectionFactory.ignore = ignore;
	}
}
