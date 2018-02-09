package com.google.code.or.binlog;

public class WrongTableMapException extends RuntimeException {
	private static final long serialVersionUID = 8105711327709452989L;

	public WrongTableMapException() {
		super("TableMapEvent object wrong and maybe it is null when parsing RowsEvent!");
		// TODO Auto-generated constructor stub
	}

	public WrongTableMapException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

	public WrongTableMapException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public WrongTableMapException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public WrongTableMapException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}
	
}
