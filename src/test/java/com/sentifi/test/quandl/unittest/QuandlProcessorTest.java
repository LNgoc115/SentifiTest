package com.sentifi.test.quandl.unittest;

import static com.sentifi.test.quandl.util.Constant.*;
import static com.sentifi.test.quandl.util.NumberUtils.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.opencsv.CSVReader;
import com.sentifi.test.quandl.connection.DataResults;
import com.sentifi.test.quandl.connection.Row;
import com.sentifi.test.quandl.processor.QuandlProcessor;

public class QuandlProcessorTest {

	DataResults csvResults;
	JSONObject jsonResults;
	DataResults dataTest;
	List<String[]> alertsResults = new ArrayList<String[]>();

	@BeforeTest
	public void initDataTest() {

		System.out.println("initDataTest start");
		// Create FB.csv, FB.json, alerts.dat
		new QuandlProcessor().process("FB");
		/*************
		 * Read file FB.csv and parse to csvResults
		 *****************/
		InputStream is = null;
		try {
			is = new FileInputStream(new File("Results" + File.separator + "FB.csv"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (is != null) {
			CSVReader reader = new CSVReader(new InputStreamReader(is));
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
							ex.printStackTrace();
						}
						if (row != null) {
							rows.add(row);
						}
						nextRow = reader.readNext();
					}
					reader.close();
					csvResults = new DataResults(headers, rows);
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		/*************
		 * End Read file FB.csv and parse to csvResults
		 *****************/

		/*************
		 * Read file FB.json and parse to jsonResults
		 *****************/
		try {
			is = new FileInputStream(new File("Results" + File.separator + "FB.json"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		JSONTokener tokeniser = new JSONTokener(new InputStreamReader(is));
		try {
			jsonResults = new JSONObject(tokeniser);
		} catch (JSONException jsone) {
			throw new RuntimeException("Problem parsing JSON reply", jsone);
		}
		/*************
		 * End Read file FB.json and parse to jsonResults
		 *************/

		/*************
		 * Read file WIKI-FB.csv and parse to dataTest
		 *****************/
		try {
			is = new FileInputStream(new File("datatest" + File.separator + "WIKI-FB.csv"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (is != null) {
			CSVReader reader = new CSVReader(new InputStreamReader(is));
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
							ex.printStackTrace();
						}
						if (row != null) {
							rows.add(row);
						}
						nextRow = reader.readNext();
					}
					reader.close();
					dataTest = new DataResults(headers, rows);
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		/*************
		 * End Read file WIKI-FB.csv and parse to dataTest
		 *****************/

		/*************
		 * Read file alerts.dat and parse to alertResults
		 *****************/
		try {
			is = new FileInputStream(new File("Results" + File.separator + "alerts.dat"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (is != null) {
			CSVReader reader = new CSVReader(new InputStreamReader(is));
			try {
				List<Row> rows = new ArrayList<Row>();
				String[] nextRow = reader.readNext();
				while (nextRow != null) {
					String[] values = nextRow;
					alertsResults.add(values);
					nextRow = reader.readNext();
				}
				reader.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		/*************
		 * End Read file alerts.dat and parse to alertResults
		 *****************/

		System.out.println("initDataTest finish");
	}

	/**
	 * unit test for task #1
	 * @throws JSONException
	 */
	@Test
	public void testForTask1() throws JSONException {
		List<Row> csvResultsRows = csvResults.getRows();
		JSONArray jsonArray = jsonResults.getJSONArray(COLUMN_NAME_PRICE);
		List<Row> dataTestRows = dataTest.getRows();

		int j = csvResultsRows.size() - 1;
		for (int i = dataTestRows.size() - 1; i >= 0; i--) {
			Row dataTestRow = dataTestRows.get(i);
			Row csvResultRow = csvResultsRows.get(j);
			JSONObject jsonRow = jsonArray.getJSONObject(j);
			j--;
			// test csv
			Assert.assertEquals(csvResultRow.getValueByColumn(COLUMN_NAME_TICKER), "FB");
			Assert.assertEquals(csvResultRow.getValueByColumn(COLUMN_NAME_DATE),
					dataTestRow.getValueByColumn(COLUMN_NAME_DATE));
			Assert.assertTrue(Math.abs(convertStringToDouble(csvResultRow.getValueByColumn(COLUMN_NAME_OPEN))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_OPEN))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(csvResultRow.getValueByColumn(COLUMN_NAME_HIGH))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_HIGH))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(csvResultRow.getValueByColumn(COLUMN_NAME_LOW))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_LOW))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(csvResultRow.getValueByColumn(COLUMN_NAME_CLOSE))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_CLOSE))) <= 0.0001);
			Assert.assertEquals(convertStringToDouble(csvResultRow.getValueByColumn(COLUMN_NAME_VOLUME)),
					convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_VOLUME)));
			Assert.assertTrue(Math.abs(convertStringToDouble(csvResultRow.getValueByColumn(COLUMN_NAME_TWAP_OPEN))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_TWAP_OPEN))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(csvResultRow.getValueByColumn(COLUMN_NAME_TWAP_HIGH))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_TWAP_HIGH))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(csvResultRow.getValueByColumn(COLUMN_NAME_TWAP_LOW))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_TWAP_LOW))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(csvResultRow.getValueByColumn(COLUMN_NAME_TWAP_CLOSE))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_TWAP_CLOSE))) <= 0.0001);

			// test json
			Assert.assertEquals(jsonRow.getString(COLUMN_NAME_TICKER), "FB");
			Assert.assertEquals(jsonRow.getString(COLUMN_NAME_DATE), dataTestRow.getValueByColumn(COLUMN_NAME_DATE));
			Assert.assertTrue(Math.abs(convertStringToDouble(jsonRow.getString(COLUMN_NAME_OPEN))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_OPEN))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(jsonRow.getString(COLUMN_NAME_HIGH))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_HIGH))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(jsonRow.getString(COLUMN_NAME_LOW))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_LOW))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(jsonRow.getString(COLUMN_NAME_CLOSE))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_CLOSE))) <= 0.0001);
			Assert.assertEquals(convertStringToDouble(jsonRow.getString(COLUMN_NAME_VOLUME)),
					convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_VOLUME)));
			Assert.assertTrue(Math.abs(convertStringToDouble(jsonRow.getString(COLUMN_NAME_TWAP_OPEN))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_TWAP_OPEN))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(jsonRow.getString(COLUMN_NAME_TWAP_HIGH))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_TWAP_HIGH))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(jsonRow.getString(COLUMN_NAME_TWAP_LOW))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_TWAP_LOW))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(jsonRow.getString(COLUMN_NAME_TWAP_CLOSE))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_TWAP_CLOSE))) <= 0.0001);

		}

	}

	/**
	 * unit test for task #2
	 * @throws JSONException
	 */
	@Test
	public void testForTask2() throws JSONException {
		List<Row> csvResultsRows = csvResults.getRows();
		List<Row> dataTestRows = dataTest.getRows();
		JSONArray jsonArray = jsonResults.getJSONArray(COLUMN_NAME_PRICE);

		int j = csvResultsRows.size() - 1;
		for (int i = dataTestRows.size() - 1; i >= 0; i--) {
			Row dataTestRow = dataTestRows.get(i);
			Row csvResultRow = csvResultsRows.get(j);
			JSONObject jsonRow = jsonArray.getJSONObject(j);
			j--;
			// test csv
			Assert.assertEquals(csvResultRow.getValueByColumn(COLUMN_NAME_TICKER), "FB");
			Assert.assertEquals(csvResultRow.getValueByColumn(COLUMN_NAME_DATE),
					dataTestRow.getValueByColumn(COLUMN_NAME_DATE));
			Assert.assertTrue(Math.abs(convertStringToDouble(csvResultRow.getValueByColumn(COLUMN_NAME_SMA50))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_SMA50))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(csvResultRow.getValueByColumn(COLUMN_NAME_SMA200))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_SMA200))) <= 0.0001);

			// test json
			Assert.assertEquals(jsonRow.getString(COLUMN_NAME_TICKER), "FB");
			Assert.assertEquals(jsonRow.getString(COLUMN_NAME_DATE), dataTestRow.getValueByColumn(COLUMN_NAME_DATE));
			Assert.assertTrue(Math.abs(convertStringToDouble(jsonRow.getString(COLUMN_NAME_SMA50))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_SMA50))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(jsonRow.getString(COLUMN_NAME_SMA200))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_SMA200))) <= 0.0001);

		}

	}

	/**
	 * unit test for task #3
	 * @throws JSONException
	 */
	@Test
	public void testForTask3() throws JSONException {
		List<Row> csvResultsRows = csvResults.getRows();
		List<Row> dataTestRows = dataTest.getRows();
		JSONArray jsonArray = jsonResults.getJSONArray(COLUMN_NAME_PRICE);

		int j = csvResultsRows.size() - 1;
		for (int i = dataTestRows.size() - 1; i >= 0; i--) {
			Row dataTestRow = dataTestRows.get(i);
			Row csvResultRow = csvResultsRows.get(j);
			JSONObject jsonRow = jsonArray.getJSONObject(j);
			j--;

			// test csv
			Assert.assertEquals(csvResultRow.getValueByColumn(COLUMN_NAME_TICKER), "FB");
			Assert.assertEquals(csvResultRow.getValueByColumn(COLUMN_NAME_DATE),
					dataTestRow.getValueByColumn(COLUMN_NAME_DATE));
			Assert.assertTrue(Math.abs(convertStringToDouble(csvResultRow.getValueByColumn(COLUMN_NAME_LWMA15))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_LWMA15))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(csvResultRow.getValueByColumn(COLUMN_NAME_LWMA50))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_LWMA50))) <= 0.0001);
			// test json
			Assert.assertEquals(jsonRow.getString(COLUMN_NAME_TICKER), "FB");
			Assert.assertEquals(jsonRow.getString(COLUMN_NAME_DATE), dataTestRow.getValueByColumn(COLUMN_NAME_DATE));
			Assert.assertTrue(Math.abs(convertStringToDouble(jsonRow.getString(COLUMN_NAME_LWMA15))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_LWMA15))) <= 0.0001);
			Assert.assertTrue(Math.abs(convertStringToDouble(jsonRow.getString(COLUMN_NAME_LWMA50))
					- convertStringToDouble(dataTestRow.getValueByColumn(COLUMN_NAME_LWMA50))) <= 0.0001);
		}

	}

	/**
	 * unit test for task #4
	 * @throws JSONException
	 */
	@Test
	public void testForTask4() {
		//test 2017-01-23
		String[] row = findRowByDateInAlerts("2017-01-23");
		Assert.assertEquals(row[0], "bearish");
		Assert.assertEquals(row[1], "FB");
		Assert.assertEquals(row[2], "2017-01-23");
		Assert.assertEquals(convertStringToDouble(row[3]), convertStringToDouble("127.31"));
		Assert.assertEquals(convertStringToDouble(row[4]), convertStringToDouble("129.25"));
		Assert.assertEquals(convertStringToDouble(row[5]), convertStringToDouble("126.95"));
		Assert.assertEquals(convertStringToDouble(row[6]), convertStringToDouble("128.93"));
		Assert.assertEquals(convertStringToDouble(row[7]), convertStringToDouble("16016924.0"));
		Assert.assertEquals(convertStringToDouble(row[8]), convertStringToDouble("120.3734"));
		Assert.assertEquals(convertStringToDouble(row[9]), convertStringToDouble("121.18805"));
		
		//test 2016-11-14
		row = findRowByDateInAlerts("2016-11-14");
		Assert.assertEquals(row[0], "bullish");
		Assert.assertEquals(row[1], "FB");
		Assert.assertEquals(row[2], "2016-11-14");
		Assert.assertEquals(convertStringToDouble(row[3]), convertStringToDouble("119.1256"));
		Assert.assertEquals(convertStringToDouble(row[4]), convertStringToDouble("119.1256"));
		Assert.assertEquals(convertStringToDouble(row[5]), convertStringToDouble("113.5535"));
		Assert.assertEquals(convertStringToDouble(row[6]), convertStringToDouble("115.08"));
		Assert.assertEquals(convertStringToDouble(row[7]), convertStringToDouble("51377040"));
		Assert.assertEquals(convertStringToDouble(row[8]), convertStringToDouble("21134655.72"));
		Assert.assertEquals(convertStringToDouble(row[9]), convertStringToDouble("127.8374"));
		Assert.assertEquals(convertStringToDouble(row[10]), convertStringToDouble("118.5285"));
	}
	
	/**
	 * 
	 * @param strDate
	 * @return
	 */
	private String[] findRowByDateInAlerts(String strDate) {
		for (String[] strArr : alertsResults) {
			if (strArr[2].equals(strDate)) {
				return strArr;
			}
		}
		return null;
	}

}
