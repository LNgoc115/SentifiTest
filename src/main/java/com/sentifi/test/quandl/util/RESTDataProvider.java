package com.sentifi.test.quandl.util;

import javax.ws.rs.client.WebTarget;

import org.json.JSONObject;

import com.sentifi.test.quandl.connection.DataResults;


public interface RESTDataProvider {

	/************** Response CODE ********************************************/
	int OK = 200;
	int EXCEED_DAILY_LIMIT = 429;
	int TOO_MANY_ROW = 422; //SOME ERRORS ALSO MAY RETURN THIS
	int TOO_LONG_PARAMETERS = 414;
	int TOO_LARGE_DATA = 413;
	int NOT_RECOGNIZE_API = 400;//SOME ERRORS ALSO MAY RETURN THIS
	int VIEW_PROTECTED_RESOURCE_IN_ANONYMOUS_MODE = 401;
	int NOT_PERMISSION_TO_USE = 403;//SOME ERRORS ALSO MAY RETURN THIS
	int INCORRECT_QUANDL_CODE = 404;//SOME ERRORS ALSO MAY RETURN THIS
	int SOMETHING_WRONG = 500;//SOME ERRORS ALSO MAY RETURN THIS
	int TOO_LONG_REQUEST = 503;
	int CONNECTION_ISSUES = 502;
	/************** End Response Codes ****************************************/

	/**
	 * @param target
	 *            the WebTarget describing the call to make, not null
	 * @return the parsed JSON object
	 */
	JSONObject getJSONResponse(final WebTarget target);

	/**
	 * @param target
	 *            the WebTarget describing the call to make, not null
	 * @return the parsed CsvResult
	 */
	DataResults getCSVResponse(final WebTarget target);

}
