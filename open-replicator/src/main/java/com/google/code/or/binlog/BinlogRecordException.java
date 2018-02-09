package com.google.code.or.binlog;

import java.io.File;

public class BinlogRecordException extends RuntimeException {
	private static final long serialVersionUID = -2334352054819747057L;

	public BinlogRecordException() {
		super("Binlog postion operating problem. please check file "
				+ System.getProperty("user.dir") + File.separator +
				"record"  + File.separator + "position.rc");
		// TODO Auto-generated constructor stub
	}

	public BinlogRecordException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

	public BinlogRecordException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public BinlogRecordException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public BinlogRecordException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}
	
}
