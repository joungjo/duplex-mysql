package com.geovis.duplex.jms;

import java.sql.SQLException;

import javax.jms.JMSException;
import javax.jms.Message;

public interface MessageHandle {
	
	public void handle(Message message) throws JMSException;
	
	public void commit();
	
	public void rollback() throws SQLException;
}
