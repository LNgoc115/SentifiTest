package com.sentifi.test.quandl.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.opencsv.CSVReader;
import com.sentifi.test.quandl.connection.DataResults;
import com.sentifi.test.quandl.connection.Row;
import com.sentifi.test.quandl.exceptions.QuandlConnectionIssuesException;
import com.sentifi.test.quandl.exceptions.QuandlExceedDailyLimitException;
import com.sentifi.test.quandl.exceptions.QuandlInvalidCodeException;
import com.sentifi.test.quandl.exceptions.QuandlNotPermissionToUseException;
import com.sentifi.test.quandl.exceptions.QuandlNotRecognizeApiException;
import com.sentifi.test.quandl.exceptions.QuandlSomethingWrongException;
import com.sentifi.test.quandl.exceptions.QuandlTooLargeDataException;
import com.sentifi.test.quandl.exceptions.QuandlTooLongParametersException;
import com.sentifi.test.quandl.exceptions.QuandlTooLongRequestException;
import com.sentifi.test.quandl.exceptions.QuandlTooManyRowException;
import com.sentifi.test.quandl.exceptions.QuandlViewProtectedResourceException;

/**
 * Get JsonObject and CsvResult from web target
 * 
 * @author NGOCLM
 *
 */
public class DefaultRESTDataProvider implements RESTDataProvider {

	Logger log = Logger.getLogger(DefaultRESTDataProvider.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sentifi.test.quandl.util.RESTDataProvider#getJSONResponse(javax.ws.rs
	 * .client.WebTarget)
	 */
	public JSONObject getJSONResponse(WebTarget target) {
		// TODO Auto-generated method stub
		Builder requestBuilder = target.request();
		Response response = requestBuilder.buildGet().invoke();
		String string = "";
		switch (response.getStatus()) {
		case OK:
			InputStream inputStream = response.readEntity(InputStream.class);
			JSONTokener tokeniser = new JSONTokener(new InputStreamReader(inputStream));
			try {
				JSONObject object = new JSONObject(tokeniser);
				return object;
			} catch (JSONException jsone) {
				throw new RuntimeException("Problem parsing JSON reply", jsone);
			}
		case EXCEED_DAILY_LIMIT:
			throw new QuandlExceedDailyLimitException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case TOO_MANY_ROW:
			throw new QuandlTooManyRowException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case TOO_LONG_PARAMETERS:
			throw new QuandlTooLongParametersException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case TOO_LARGE_DATA:
			throw new QuandlTooLargeDataException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case NOT_RECOGNIZE_API:
			throw new QuandlNotRecognizeApiException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case VIEW_PROTECTED_RESOURCE_IN_ANONYMOUS_MODE:
			throw new QuandlViewProtectedResourceException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case NOT_PERMISSION_TO_USE:
			throw new QuandlNotPermissionToUseException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case INCORRECT_QUANDL_CODE:
			throw new QuandlInvalidCodeException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case SOMETHING_WRONG:
			throw new QuandlSomethingWrongException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case TOO_LONG_REQUEST:
			throw new QuandlTooLongRequestException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case CONNECTION_ISSUES:
			throw new QuandlConnectionIssuesException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		default:
			throw new RuntimeException("Response code to " + target.getUri() + " was " + response.getStatusInfo());

		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sentifi.test.quandl.util.RESTDataProvider#getCSVResponse(javax.ws.rs.
	 * client.WebTarget)
	 */
	public DataResults getCSVResponse(WebTarget target) {
		// TODO Auto-generated method stub
		Builder requestBuilder = target.request();
		Response response = requestBuilder.buildGet().invoke();
		switch (response.getStatus()) {
		case OK:
			InputStream inputStream = response.readEntity(InputStream.class);
			CSVReader reader = new CSVReader(new InputStreamReader(inputStream));
			try {
				String[] headerRow = reader.readNext();
				if (headerRow != null) {
					List<String> headers = Arrays.asList(headerRow);
					List<Row> rows = new ArrayList<Row>();
					String[] nextRow = reader.readNext();
					while (nextRow != null) {
						Row row = null;
						try {
							row = new Row(headers, nextRow);
						} catch (RuntimeException ex) {
							// in case: headers length and nextRow length not
							// map
							log.error(ex.getMessage(), ex);
						}
						if (row != null) {
							rows.add(row);
						}
						nextRow = reader.readNext();
					}
					reader.close();
					return new DataResults(headers, rows);
				} else {
					return null;
				}
			} catch (IOException ex) {
				throw new RuntimeException(ex);
			}

		case EXCEED_DAILY_LIMIT:
			throw new QuandlExceedDailyLimitException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case TOO_MANY_ROW:
			throw new QuandlTooManyRowException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case TOO_LONG_PARAMETERS:
			throw new QuandlTooLongParametersException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case TOO_LARGE_DATA:
			throw new QuandlTooLargeDataException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case NOT_RECOGNIZE_API:
			throw new QuandlNotRecognizeApiException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case VIEW_PROTECTED_RESOURCE_IN_ANONYMOUS_MODE:
			throw new QuandlViewProtectedResourceException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case NOT_PERMISSION_TO_USE:
			throw new QuandlNotPermissionToUseException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case INCORRECT_QUANDL_CODE:
			throw new QuandlInvalidCodeException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case SOMETHING_WRONG:
			throw new QuandlSomethingWrongException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case TOO_LONG_REQUEST:
			throw new QuandlTooLongRequestException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		case CONNECTION_ISSUES:
			throw new QuandlConnectionIssuesException(
					"Response code to " + target.getUri() + " was " + response.getStatusInfo());
		default:
			throw new RuntimeException("Response code to " + target.getUri() + " was " + response.getStatusInfo());
		}
	}

}
