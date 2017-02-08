package com.sentifi.test.quandl.connection;

import java.util.Iterator;
import java.util.List;

/**
 * 
 * 
 * @author NGOCLM
 *
 */
public class DataResults implements Iterable<Row> {

	/** header of each column. */
	private List<String> headers;
	/** list of rows. */
	private List<Row> rows;
	
	/**
	 * 
	 * @param headers
	 * @param rows
	 */
	public DataResults(List<String> headers, List<Row> rows) {
		super();
		this.headers = headers;
		this.rows = rows;
	}
	
	public DataResults() {
		super();
	}


	/**
	 * 
	 * @return size of rows
	 */
	int size() {
		return rows.size();
	}

	/**
	 * 
	 * @return rows isEmpty
	 */
	boolean isEmpty() {
		return rows.isEmpty();
	}

	public Iterator<Row> iterator() {
		// TODO Auto-generated method stub
		return rows.iterator();
	}


	public List<Row> getRows() {
		return rows;
	}
	

	public List<String> getHeaders() {
		return headers;
	}
	

	public void setHeaders(List<String> headers) {
		this.headers = headers;
	}

	public void setRows(List<Row> rows) {
		this.rows = rows;
	}

	@Override
	public String toString() {
		return "DataResults [headers=" + headers + ", rows=" + rows + "]";
	}
	

}
