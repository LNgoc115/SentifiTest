package com.sentifi.test.quandl.connection;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represent a single row from csv response
 * @author NGOCLM
 *
 */
public class Row {
	
	private List<String> headers;
	private String[] values;
	private Map<String, Integer> mapColumnNameToIndices = new HashMap<String, Integer>();
	/**
	 * 
	 * @param headers
	 * @param values
	 */
	public Row(List<String> headers, String[] values) {
		super();
		this.headers = headers;
		this.values = values;
		if (headers.size() != values.length) {
			throw new RuntimeException("Headers and values not map!");
		}
		if (headers != null) {
			for (int i = 0; i < headers.size(); i++) {
				mapColumnNameToIndices.put(headers.get(i), i);
			}
		}
	}
	
	/**
	 * 
	 * @param columnName
	 * @return row value at columnName index. Return null if not exist columnName
	 */
	public String getValueByColumn(String columnName) {
		Integer index = mapColumnNameToIndices.get(columnName.trim());
		if (index == null) return null;
		return values[index];
	}
	
	public List<String> getHeaders() {
		return headers;
	}
	public String[] getValues() {
		return values;
	}
	@Override
	public String toString() {
		return "Row [headers=" + headers + ", values=" + Arrays.toString(values) + "]";
	}
	
}
