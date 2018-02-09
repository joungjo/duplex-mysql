package com.geovis.duplex.mysql.receive;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import com.geovis.duplex.mysql.handle.MysqlHandle;
import com.geovis.duplex.mysql.model.NodeModel;
import com.geovis.duplex.util.AbstractStatusRunnable;

public class MysqlReceive extends AbstractStatusRunnable {
	private NodeModel node;
	private Session session;
	public MysqlReceive() {
	}

	public MysqlReceive(NodeModel model) {
		this.node = model;
	}

	@Override
	public void run() {
		String ip = node.getIp();
		
		StringBuilder sb = new StringBuilder("failover:(tcp://");
		sb.append(ip).append(":").append(node.getPort())
		.append("?wireFormat.maxInactivityDuration=10000)&amp;maxReconnectDelay=10000");
		ActiveMQConnectionFactory factory = 
				new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER,
				ActiveMQConnection.DEFAULT_PASSWORD, 
				sb.toString());
		
		MysqlHandle handle = new MysqlHandle(this, node);
		try {
			Connection connection = factory.createConnection();
			connection.setClientID(node.getClientId());
			connection.start();
			session = connection.createSession(Boolean.TRUE, Session.SESSION_TRANSACTED);
			Topic topic = session.createTopic(node.getServerId());
			MessageConsumer consumer = session.createDurableSubscriber(topic, node.getServerId());
			while (status) {
				handle.handle(consumer.receive());
			}
			handle.commit();
			session.commit();
			consumer.close();
			session.close();
			connection.stop();
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		} 
	}

	public NodeModel getNode() {
		return node;
	}

	public void setNode(NodeModel node) {
		this.node = node;
	}

	public void commit() {
		try {
			session.commit();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	public void rollback() {
		try {
			session.rollback();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

}
