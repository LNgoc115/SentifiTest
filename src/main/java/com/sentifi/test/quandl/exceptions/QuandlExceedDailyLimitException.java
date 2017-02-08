package com.sentifi.test.quandl.exceptions;

public class QuandlExceedDailyLimitException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public QuandlExceedDailyLimitException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public QuandlExceedDailyLimitException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}
	
	

}
