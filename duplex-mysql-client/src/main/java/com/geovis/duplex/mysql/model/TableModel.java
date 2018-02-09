package com.geovis.duplex.mysql.model;

import java.util.Map;

public class TableModel {
	String name;
	String localSchema;
	String localTable;
	private String schema;
	Map<String, String> fieldPairs;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getLocalTable() {
		return localTable;
	}
	public void setLocalTable(String localTable) {
		this.localTable = localTable;
	}
	public Map<String, String> getFieldPairs() {
		return fieldPairs;
	}
	public void setFieldPairs(Map<String, String> fieldPairs) {
		this.fieldPairs = fieldPairs;
	}
	public String getLocalSchema() {
		return localSchema;
	}
	public void setLocalSchema(String localSchema) {
		this.localSchema = localSchema;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(super.toString());
		sb.append("{name:").append(name).append(",localSchema:")
		.append(localSchema).append(",localTable:").append(localTable)
		.append(",fieldPars:").append(fieldPairs);
		return sb.toString();
	}
}
