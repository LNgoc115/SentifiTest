package com.sentifi.test.quandl.exceptions;


public class QuandlRequestFailedException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Constructor when another exception is being included.
	 * 
	 * @param message
	 *            a message describing the exception, not null
	 * @param cause
	 *            the cause of the expection if there is one, not null
	 */
	public QuandlRequestFailedException(final String message, final Throwable cause) {
		super(message, cause);
	}

	/**
	 * Constructor when exception is not caused by an underlying exception.
	 * 
	 * @param message
	 *            a message describing the exception, not null
	 */
	public QuandlRequestFailedException(final String message) {
		super(message);
	}
}
