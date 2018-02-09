package com.geovis.duplex;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.geovis.duplex.mysql.driver.ConnectionFactory;
import com.geovis.duplex.mysql.model.NodeModel;
import com.geovis.duplex.mysql.model.TableModel;
import com.geovis.duplex.mysql.receive.MysqlReceive;

public class MysqlClient {
	
	private MysqlClient() {
		try {
			this.load(System.getProperty("user.dir") + "/conf/configs.xml");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (DocumentException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public void load(String filePath) throws DocumentException, ClassNotFoundException {
		SAXReader reader = new SAXReader();
		Document document = reader.read(new File(filePath));
		
		Element root = document.getRootElement();
		
		String clientId = root.valueOf("clientId");
		String user = root.valueOf("user");
		String password = root.valueOf("password");
		String url = root.valueOf("url");
		String driver = root.valueOf("driver");
		String ignore = root.valueOf("binlog-ignore");
		Class.forName(driver);
		ConnectionFactory.setUrl(url);
		ConnectionFactory.setUser(user);
		ConnectionFactory.setPassword(password);
		ConnectionFactory.setIgnore(ignore);
		
		Iterator<Element> iterator = root.elementIterator("node");
		//databases
		while(iterator.hasNext()) {
			Element node = iterator.next();
			String ip = node.attributeValue("ip");
			String port = node.attributeValue("port");
			String serverId = node.attributeValue("serverId");
			
			NodeModel model = new NodeModel();
			model.setClientId(clientId);
			model.setIp(ip);
			model.setServerId(serverId);
			model.setPort(Integer.parseInt(port));
			
			Map<String, List<TableModel>> schemas = new HashMap<>();
			Iterator<Element> databases = node.elementIterator();
			//tables
			while(databases.hasNext()) {
				List<TableModel> tableModels = new ArrayList<>();
				Element database = databases.next();
				String databaseName = database.attributeValue("name");
				Iterator<Element> tables = database.elementIterator();
				while(tables.hasNext()){
					Element table = tables.next();
					TableModel tableModel = new TableModel();
					
					String tableName = table.attributeValue("name");
					tableModel.setName(tableName);
					
					String localSchema = table.attributeValue("localSchema");
					if (StringUtils.isNotEmpty(localSchema)) {
						tableModel.setLocalSchema(localSchema);
					} else {
						tableModel.setLocalSchema(databaseName);
					}
					
					String localTable = table.attributeValue("localTable");
					if (StringUtils.isNotEmpty(localTable)) {
						tableModel.setLocalTable(localTable);
					} else {
						tableModel.setLocalTable(tableName);
					}
					
					Iterator<Element> fields = table.elementIterator();
					Map<String, String> fieldPairs = new HashMap<>();
					while(fields.hasNext()){
						Element field = fields.next();
						String remote = field.valueOf("remote");
						String local = field.valueOf("local");
						if (remote == null || local == null 
								|| remote.trim().equals("") 
								|| local.trim().equals("")) {
							throw new RuntimeException("field pair lacks another element");
						}
						fieldPairs.put(local, remote);
					}
					tableModel.setFieldPairs(fieldPairs);
					tableModels.add(tableModel);
				}
				schemas.put(databaseName, tableModels);
			}
			model.setSchemas(schemas);
			System.out.println(model);
			new Thread(new MysqlReceive(model)).start();
		}
	}

	//singleton
	private final static MysqlClient client = new MysqlClient();
	
	public static MysqlClient getInstance() {
		return client;
	}

	public static void main(String[] args) {}
}
