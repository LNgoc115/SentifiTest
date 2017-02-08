package com.sentifi.test.quandl.util;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * 
 * @author NGOCLM
 *
 */
public class JSonUtils {

	/**
	 * Convert from jsonArray that contains string value to list String
	 * @param jsonArray
	 * @return List<String>
	 * @throws JSONException
	 */
	public static List<String> convertJSONArrayToStringList(JSONArray jsonArray) throws JSONException {
		List<String> results = new ArrayList<String>();
		if (jsonArray != null) {
			for (int i = 0; i < jsonArray.length(); i++) {
				results.add(jsonArray.getString(i));
			}
		}
		return results;
	}
	
	/**
	 * Convert from jsonArray that contains string value to array String
	 * @param jsonArray
	 * @return  String[]
	 * @throws JSONException 
	 */
	public static String[] convertJSONArrayToStringArray(JSONArray jsonArray) throws JSONException {
		String[] results = null;
		if (jsonArray != null) {
			results = new String[jsonArray.length()];
			for (int i = 0; i < jsonArray.length(); i++) {
				results[i] = jsonArray.getString(i);
			}
		}
		return results;
	}

}
