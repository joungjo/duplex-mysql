package com.geovis.duplex.jms;

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import com.geovis.duplex.jms.broker.EmbededBroker;
import com.geovis.duplex.utils.Utils;

public class JmsPusher implements Pusher {
	private static final ActiveMQConnectionFactory localFactory = 
			new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER,
					ActiveMQConnection.DEFAULT_PASSWORD, 
					"vm://Broker");
	static {
		EmbededBroker broker = new EmbededBroker();
		broker.start();
		localFactory.setUseAsyncSend(true);
		localFactory.setCopyMessageOnSend(false);
		localFactory.setAlwaysSessionAsync(false);
	}
	
	private Connection connection;
	private Session session;
	private MessageProducer producer;
	
	{
		try {
			connection = localFactory.createConnection();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			StringBuilder topicName = new StringBuilder();
			topicName.append(Utils.getProperty("server.id"));
			topicName.append("?consumer.retroactive=true");
			
			Topic topic = session.createTopic(topicName.toString());
			producer = session.createProducer(topic);
			
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	public JmsPusher() {
	}

	@Override
	public void push(Serializable o) {
		try {
			Message message = session.createObjectMessage(o);
			producer.send(message);
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		try {
//			session.commit();
			session.close();
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

}
