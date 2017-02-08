package com.sentifi.test.quandl.connection;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.sentifi.test.quandl.util.ArgumentChecker;

/**
 * Create the session connect to Quandl
 * 
 * @author NGOCLM
 *
 */
public class QuandlSession {
	// Log
	Logger log = Logger.getLogger(QuandlSession.class);
	// Options to connect to Quandl
	private SessionOptions sessionOptions;

	private static final String API_BASE_URL = "https://www.quandl.com/api/v3/datasets/WIKI";

	private static final String JSON_EXTENSION = ".json";

	private static final String CSV_EXTENSION = ".csv";

	public QuandlSession(SessionOptions sessionOptions) {
		super();
		this.sessionOptions = sessionOptions;
	}

	/**
	 * Create QuandlSession with no authenKey API
	 * 
	 * @return QuandlSession
	 */
	public static QuandlSession create() {
		SessionOptions _sessionOptions = new SessionOptions();
		return new QuandlSession(_sessionOptions);
	}

	/**
	 * 
	 * @return Client
	 */
	private Client getClient() {
		return ClientBuilder.newClient();
	}

	/**
	 * Get JSON results of quandlCode from Quandl URL
	 * 
	 * @param quandlCode
	 * @return JSonResults
	 */
	public JSonResults getJSonResults(String quandlCode) {
		ArgumentChecker.notNullOrEmpty(quandlCode, "quandlCode");
		Client client = getClient();
		WebTarget target = client.target(API_BASE_URL);
		// add quandlCode (ex: WIKI/FB.json) to path
		target = target.path(quandlCode + JSON_EXTENSION);
		JSonResults results = null;
		JSONObject jsonObject = null;
		int retries = 0;
		do {
			try {
				jsonObject = sessionOptions.getRestDataProvider().getJSONResponse(target);
			} catch (RuntimeException ex) {
				// TODO: handle exception
				log.error(ex.getMessage(), ex);
				ex.printStackTrace();
			}

			if (jsonObject != null) {
				results = new JSonResults(jsonObject);
			}
		} while (results == null && sessionOptions.getRetryPolicy().checkRetries(retries++));
		return results;
	}
	
	public JSONObject getJSonObject(String quandlCode) {
		ArgumentChecker.notNullOrEmpty(quandlCode, "quandlCode");
		Client client = getClient();
		WebTarget target = client.target(API_BASE_URL);
		// add quandlCode (ex: WIKI/FB.json) to path
		target = target.path(quandlCode + JSON_EXTENSION);
		JSonResults results = null;
		JSONObject jsonObject = null;
		int retries = 0;
		do {
			try {
				jsonObject = sessionOptions.getRestDataProvider().getJSONResponse(target);
			} catch (RuntimeException ex) {
				// TODO: handle exception
				log.error(ex.getMessage(), ex);
				ex.printStackTrace();
			}

			if (jsonObject != null) {
				results = new JSonResults(jsonObject);
			}
		} while (results == null && sessionOptions.getRetryPolicy().checkRetries(retries++));
		return jsonObject;
	}

	/**
	 * Get CSV results of quandlCode from Quandl URL
	 * 
	 * @param quandlCode
	 * @return
	 */
	public DataResults getCsvResults(String quandlCode) {
		ArgumentChecker.notNullOrEmpty(quandlCode, "quandlCode");
		Client client = getClient();
		WebTarget target = client.target(API_BASE_URL);
		// add quandlCode (ex: WIKI/FB.csv) to path
		target = target.path(quandlCode + CSV_EXTENSION);
		DataResults results = null;
		int retries = 0;
		do {
			try {
				results = sessionOptions.getRestDataProvider().getCSVResponse(target);
			} catch (RuntimeException ex) {
				// TODO: handle exception
				log.error(ex.getMessage(), ex);
			}

		} while (results == null && sessionOptions.getRetryPolicy().checkRetries(retries++));
		return results;
	}

}
