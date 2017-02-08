package com.sentifi.test.quandl.util;

import java.util.Collection;

import org.apache.log4j.Logger;

public final class ArgumentChecker {
	
	private static Logger log =  Logger.getLogger(ArgumentChecker.class);

	private ArgumentChecker() {
	}

	/**
	 * Throws an exception if the argument is not null.
	 * 
	 * @param argument
	 *            the object to check
	 * @param name
	 *            the name of the parameter
	 */
	public static void notNull(final Object argument, final String name) {
		if (argument == null) {
			log.error("Argument " + name + " was null");
			throw new RuntimeException("Value " + name + " was null");
		}
	}

	/**
	 * Throws an exception if the array argument is not null or empty.
	 * 
	 * @param <E>
	 *            type of array
	 * @param argument
	 *            the object to check
	 * @param name
	 *            the name of the parameter
	 */
	public static <E> void notNullOrEmpty(final E[] argument, final String name) {
		if (argument == null) {
			log.error("Argument " + name + " was null");
			throw new RuntimeException("Value " + name + " was null");
		} else if (argument.length == 0) {
			log.error("Argument  " + name + " was empty array");
			throw new RuntimeException("Value " + name + " was empty array");
		}
	}

	/**
	 * Throws an exception if the collection argument is not null or empty.
	 * 
	 * @param <E>
	 *            type of array
	 * @param argument
	 *            the object to check
	 * @param name
	 *            the name of the parameter
	 */
	public static <E> void notNullOrEmpty(final Collection<E> argument, final String name) {
		if (argument == null) {
			log.error("Argument " + name + " was null");
			throw new RuntimeException("Value " + name + " was null");
		} else if (argument.size() == 0) {
			log.error("Argument " + name + " was empty collection");
			throw new RuntimeException("Value " + name + " was empty collection");
		}
	}

	/**
	 * Throws an exception if the string argument is not null or empty.
	 * 
	 * @param argument
	 *            the String to check
	 * @param name
	 *            the name of the parameter
	 */
	public static void notNullOrEmpty(final String argument, final String name) {
		if (argument == null) {
			log.error("Argument " + name + " was null");
			throw new RuntimeException("Value " + name + " was null");
		} else if (argument.length() == 0) {
			log.error("Argument " + name + " was empty string");
			throw new RuntimeException("Value " + name + " was empty string");
		}
	}
}
