package com.google.code.or.common.glossary.column;

import com.google.code.or.common.glossary.Column;

public abstract class AbstractColumn implements Column {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5050101200589918183L;
	
	private byte[] bytes;
	private int length;
	
	
	
	public byte[] getBytes() {
		return this.bytes;
	}
	
	public int getLength() {
		return length;
	}

}
