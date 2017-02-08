package com.sentifi.test.quandl.connection;

import static com.sentifi.test.quandl.util.JSonUtils.*;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * JSON format data
 * 
 * @author NGOCLM
 *
 */
public class JSonResults extends DataResults {

	// raw JSon Object
	private JSONObject jSonObject;

	private static final String DATASET_FIELD = "dataset";
	private static final String START_DATE_FIELD = "start_date";
	private static final String END_DATE_FIELD = "end_date";
	private static final String COLUMN_NAMES_FIELD = "column_names";
	private static final String DATA_FIELD = "data";

	public JSonResults(JSONObject jSonObject) {
		this.jSonObject = jSonObject;
		init();
	}

	/**
	 * init headers and rows value to JSonResults from jSonObject
	 */
	private void init() {
		try {
			JSONObject datasetObject = this.jSonObject.getJSONObject(DATASET_FIELD);
			JSONArray columnArray = datasetObject.getJSONArray(COLUMN_NAMES_FIELD);
			//init headers value
			List<String> columnNames = convertJSONArrayToStringList(columnArray);
			this.setHeaders(columnNames);
			JSONArray dataArray = datasetObject.getJSONArray(DATA_FIELD);
			//init rows value
			List<Row> rows = new ArrayList<Row>();
			for (int i = 0; i < dataArray.length(); i++) {
				JSONArray rowArray = dataArray.getJSONArray(i);
				String[] rowValue = convertJSONArrayToStringArray(rowArray);
				Row row = new Row(columnNames, rowValue);
				rows.add(row);
			}
			this.setRows(rows);
		} catch (Exception ex) {
			throw new RuntimeException("Unexpect structure JSON Object", ex);
		}
	}
	
	public JSONObject getjSonObject() {
		return jSonObject;
	}

	public void setjSonObject(JSONObject jSonObject) {
		this.jSonObject = jSonObject;
	}

	@Override
	public String toString() {
		return "JSonResults [jSonObject=" + jSonObject.toString() + "]";
	}

}
