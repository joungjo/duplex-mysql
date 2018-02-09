package com.geovis.duplex.mysql.model;

import java.util.List;
import java.util.Map;

public class NodeModel {
	private String clientId;
	private String ip;
	private int port;
	private String serverId;
	
	Map<String, List<TableModel>> schemas;
	public NodeModel() {
		// TODO Auto-generated constructor stub
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public Map<String, List<TableModel>> getSchemas() {
		return schemas;
	}
	public void setSchemas(Map<String, List<TableModel>> schemas) {
		this.schemas = schemas;
	}
	public String getClientId() {
		return clientId;
	}
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
	public String getServerId() {
		return serverId;
	}
	public void setServerId(String serverId) {
		this.serverId = serverId;
	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(super.toString());
		sb.append("{clientId:").append(clientId).append(",ip:")
		.append(ip).append(",port:").append(port).append(",ignore:")
		.append(",schemas:").append(schemas)
		.append(",serverId:").append(serverId);
		return sb.toString();
	}
}
