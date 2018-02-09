package com.geovis.duplex.test;

import com.geovis.duplex.utils.Utils;

public class TestFilter {

	public static void main(String[] args) {
		MysqlExtractor extractor = new MysqlExtractor();
		extractor.setUser(Utils.getProperty("mysql.user"));
		extractor.setPassword(Utils.getProperty("mysql.password"));
		extractor.setHost(Utils.getProperty("mysql.ip"));
		extractor.setPort(Integer.parseInt(Utils.getProperty("mysql.port")));
		extractor.setServerId(Integer.parseInt(Utils.getProperty("server.id")));
		extractor.setBinlogPosition(Integer.parseInt(Utils.getProperty("binlog.position")));
		extractor.setBinlogFileName(Utils.getProperty("binlog.file"));
		extractor.setIgnoreDatabase(Utils.getProperty("mysql.ignore.database"));
		extractor.start();
	}

}
