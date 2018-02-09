package com.google.code.or.binlog;

import java.io.Serializable;
/**
 * 
 * @author jangzo
 *
 */
public interface EventContext {
	
	public Object get();
	
	public void set(Object object);

	public void doSomething(Serializable o);

}