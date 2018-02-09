package com.geovis.duplex;

import com.geovis.duplex.mysql.MysqlExtractor;
import com.geovis.duplex.utils.Utils;

public class MysqlServer {
	private MysqlExtractor extractor = new MysqlExtractor();
	public MysqlServer() { }

	public static void main(String[] args) {
		MysqlServer duplicator = new MysqlServer();
		duplicator.start();
	}

	public void start() {
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
