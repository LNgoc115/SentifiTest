package com.sentifi.test.quandl.processor;

import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_CLOSE;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_DATE;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_HIGH;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_LOW;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_LWMA15;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_LWMA50;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_OPEN;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_PRICE;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_SMA200;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_SMA50;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_TICKER;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_TWAP_CLOSE;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_TWAP_HIGH;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_TWAP_LOW;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_TWAP_OPEN;
import static com.sentifi.test.quandl.util.Constant.COLUMN_NAME_VOLUME;
import static com.sentifi.test.quandl.util.Constant.LWMA15_LENGTH;
import static com.sentifi.test.quandl.util.Constant.LWMA50_LENGTH;
import static com.sentifi.test.quandl.util.Constant.SMA200_LENGTH;
import static com.sentifi.test.quandl.util.Constant.SMA50_LENGTH;
import static com.sentifi.test.quandl.util.Constant.VOLUME50_LENGTH;
import static com.sentifi.test.quandl.util.NumberUtils.convertStringToDouble;
import static com.sentifi.test.quandl.util.NumberUtils.formatDoubleValue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.sentifi.test.quandl.connection.DataResults;
import com.sentifi.test.quandl.connection.QuandlSession;
import com.sentifi.test.quandl.connection.Row;
import com.sentifi.test.quandl.util.ArgumentChecker;
import com.sentifi.test.quandl.util.QuandlCaculation;
import com.sentifi.test.quandl.util.QuandlProcessUtils;

/**
 * process: read, transfer, process, print data from quandl url
 * @author NGOCLM
 *
 */
public class QuandlProcessor {
	
	Logger log = Logger.getLogger(QuandlProcessor.class);

	// Contain data for all history information following csv sentifi format
	private DataResults csvDataResult;

	// Contain data for alert
	private List<String[]> alertDataResult = new ArrayList<String[]>();

	// contain data for all history information following json sentifi format
	private JSONObject jsonDataResult = new JSONObject();

	/**
	 * create the csv object following format: ticker symbol,
	 * "Date","Open","High","Low","Close","Volume", “TWAP-Open”, “TWAP-High”,
	 * “TWAP-Low”, “TWAP-Close”.. and generate alert data
	 * 
	 * @param rawCsv
	 * @return object of csv contain headers and rows follow the request of the
	 *         test
	 */
	private void processQuandlData(String quandlCode, DataResults rawCsv) {
		ArgumentChecker.notNull(quandlCode, "quandlCode");
		ArgumentChecker.notNull(rawCsv, "rawCsv");

		List<Row> rawRows = rawCsv.getRows();
		// create headers of csv object
		List<String> csvHeaders = generateHistoryHeaders();
		//create rows of csv object
		List<Row> csvRows = new ArrayList<Row>();

		// create json array
		JSONArray priceJsonArray = new JSONArray();

		// calculate twa sma, lwma
		QuandlCaculation qc = new QuandlCaculation();
		qc.init(rawRows);

		Double sma50 = 0D;
		Double next50ClosePrice = 0D;
		Double sma200 = 0D;
		Double next200ClosePrice = 0D;
		Double lwma15 = 0D;
		Double next15ClosePrice = 0D;
		Double lwma50 = 0D;
		Double twaOpen = 0D;
		Double twaHigh = 0D;
		Double twaLow = 0D;
		Double twaClose = 0D;

		Row next50Row = null;
		Row next200Row = null;
		Row next15Row = null;

		Double volume50 = 0D;
		Double next50Volume = 0D;

		for (int i = 0; i < rawRows.size(); i++) {
			Row row = rawRows.get(i);

			// Calculate TWA open
			twaOpen = qc.calculateTWAOpen(row);
			// Calculate TWA high
			twaHigh = qc.calculateTWAHigh(row);
			// Calculate TWA low
			twaLow = qc.calculateTWALow(row);
			// Calculate TWA close
			twaClose = qc.calculateTWAClose(row);
			// minus twaNum 1
			qc.minusTWANum();

			/**********************
			 * Caculate average 50 day VOLUME value
			 ************/
			volume50 = 0D;
			if ((i + VOLUME50_LENGTH - 1) < rawRows.size()) {
				// get the next 50 row
				next50Row = rawRows.get(i + VOLUME50_LENGTH - 1);
				// get the next 50 volume
				next50Volume = convertStringToDouble(next50Row.getValueByColumn(COLUMN_NAME_VOLUME));
				// add the next 50 volume to queue
				qc.addNumberToVolume50Queue(next50Volume);
				// calculate average 50 day volume
				volume50 = qc.calculateVolume50();
			}
			/********************
			 * End Caculate average 50 day VOLUME value
			 ********/

			/********************** Caculate SMA 50 value ************/
			sma50 = 0D;
			if ((i + SMA50_LENGTH - 1) < rawRows.size()) {
				// get the next 50 row
				next50Row = rawRows.get(i + SMA50_LENGTH - 1);
				// get the next 50 close price
				next50ClosePrice = convertStringToDouble(next50Row.getValueByColumn(COLUMN_NAME_CLOSE));
				// add the next 50 close price to queue
				qc.addNumberToSMA50Queue(next50ClosePrice);
				// calculate sma 50
				sma50 = qc.calculateSMA50();
			}
			/******************** End Calculate SMA50 value ********/

			/********************** Caculate SMA 200 value ************/
			sma200 = 0D;
			if ((i + SMA200_LENGTH - 1) < rawRows.size()) {
				// get the next 200 row
				next200Row = rawRows.get(i + SMA200_LENGTH - 1);
				// get the next 200 close price
				next200ClosePrice = convertStringToDouble(next200Row.getValueByColumn(COLUMN_NAME_CLOSE));
				// add the next 200 close price to queue
				qc.addNumberToSMA200Queue(next200ClosePrice);
				// calculate sma 200
				sma200 = qc.calculateSMA200();
			}
			/******************** End Calculate SMA200 value ********/

			/******************** Calculate LWMA15 value *************/
			lwma15 = 0D;
			if ((i + LWMA15_LENGTH - 1) < rawRows.size()) {
				// get the next 15 row
				next15Row = rawRows.get(i + LWMA15_LENGTH - 1);
				// get the next 15 close price
				next15ClosePrice = convertStringToDouble(next15Row.getValueByColumn(COLUMN_NAME_CLOSE));
				// add the next 15 close price to queue
				qc.addNumberToLWMA15Queue(next15ClosePrice);
				// calculate lwma15
				lwma15 = qc.calculateLWMA15();
			}
			/******************** End Calculate LWMA15 value *********/

			/******************** Calculate LWMA50 value *************/
			lwma50 = 0D;
			if ((i + LWMA50_LENGTH - 1) < rawRows.size()) {
				// get the next 50 row
				next50Row = rawRows.get(i + LWMA50_LENGTH - 1);
				// get the next 50 close price
				next50ClosePrice = convertStringToDouble(next50Row.getValueByColumn(COLUMN_NAME_CLOSE));
				// add the next 50 close price to queue
				qc.addNumberToLWMA50Queue(next50ClosePrice);
				// calculate lwma50
				lwma50 = qc.calculateLWMA50();
			}
			/******************** End Calculate LWMA50 value *********/

			// Create values for csv row
			String[] values = new String[] { quandlCode, row.getValueByColumn(COLUMN_NAME_DATE),
					row.getValueByColumn(COLUMN_NAME_OPEN), row.getValueByColumn(COLUMN_NAME_HIGH),
					row.getValueByColumn(COLUMN_NAME_LOW), row.getValueByColumn(COLUMN_NAME_CLOSE),
					row.getValueByColumn(COLUMN_NAME_VOLUME), formatDoubleValue(twaOpen), formatDoubleValue(twaHigh),
					formatDoubleValue(twaLow), formatDoubleValue(twaClose), formatDoubleValue(sma50),
					formatDoubleValue(sma200), formatDoubleValue(lwma15), formatDoubleValue(lwma50) };
			// create the csv row
			Row _row = new Row(csvHeaders, values);
			csvRows.add(_row);

			// craete the jsonobject
			JSONObject object = generateJSONObject(_row);
			priceJsonArray.put(object);

			// generate data for alert
			checkAndAlert(_row, sma50, sma200, volume50);
		}
		//create csv data object result
		csvDataResult = new DataResults(csvHeaders, csvRows);
		//create json data object result
		try {
			if (priceJsonArray.length() > 0) {
				jsonDataResult.put(COLUMN_NAME_PRICE, priceJsonArray);
			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * create json object from a row
	 * 
	 * @param row
	 * @return json object
	 */
	private JSONObject generateJSONObject(Row row) {
		LinkedHashMap<String, String> jsonOrderedMap = new LinkedHashMap<String, String>();
		jsonOrderedMap.put(COLUMN_NAME_TICKER, row.getValueByColumn(COLUMN_NAME_TICKER));
		jsonOrderedMap.put(COLUMN_NAME_DATE, row.getValueByColumn(COLUMN_NAME_DATE));
		jsonOrderedMap.put(COLUMN_NAME_OPEN, row.getValueByColumn(COLUMN_NAME_OPEN));
		jsonOrderedMap.put(COLUMN_NAME_HIGH, row.getValueByColumn(COLUMN_NAME_HIGH));
		jsonOrderedMap.put(COLUMN_NAME_LOW, row.getValueByColumn(COLUMN_NAME_LOW));
		jsonOrderedMap.put(COLUMN_NAME_CLOSE, row.getValueByColumn(COLUMN_NAME_CLOSE));
		jsonOrderedMap.put(COLUMN_NAME_VOLUME, row.getValueByColumn(COLUMN_NAME_VOLUME));
		jsonOrderedMap.put(COLUMN_NAME_TWAP_OPEN, row.getValueByColumn(COLUMN_NAME_TWAP_OPEN));
		jsonOrderedMap.put(COLUMN_NAME_TWAP_HIGH, row.getValueByColumn(COLUMN_NAME_TWAP_HIGH));
		jsonOrderedMap.put(COLUMN_NAME_TWAP_LOW, row.getValueByColumn(COLUMN_NAME_TWAP_LOW));
		jsonOrderedMap.put(COLUMN_NAME_TWAP_CLOSE, row.getValueByColumn(COLUMN_NAME_TWAP_CLOSE));

		jsonOrderedMap.put(COLUMN_NAME_SMA50, row.getValueByColumn(COLUMN_NAME_SMA50));
		jsonOrderedMap.put(COLUMN_NAME_SMA200, row.getValueByColumn(COLUMN_NAME_SMA200));

		jsonOrderedMap.put(COLUMN_NAME_LWMA15, row.getValueByColumn(COLUMN_NAME_LWMA15));
		jsonOrderedMap.put(COLUMN_NAME_LWMA50, row.getValueByColumn(COLUMN_NAME_LWMA50));
		JSONObject object = new JSONObject(jsonOrderedMap);
		return object;
	}

	/**
	 * check alert condition if true generate data for alert
	 * 
	 * @param volume50
	 * @param sma200
	 * @param sma50
	 * @param row
	 */
	private void checkAndAlert(Row row, Double sma50, Double sma200, Double volume50) {

		Double currentVolume = convertStringToDouble(row.getValueByColumn(COLUMN_NAME_VOLUME));
		// check 50 SMA < 200 SMA
		if ((sma50 > 0) && (sma200 > 0) && sma50 < sma200) {
			// create the data for bearish alert
			String[] values = new String[] { "bearish", row.getValueByColumn(COLUMN_NAME_TICKER),
					row.getValueByColumn(COLUMN_NAME_DATE), row.getValueByColumn(COLUMN_NAME_OPEN),
					row.getValueByColumn(COLUMN_NAME_HIGH), row.getValueByColumn(COLUMN_NAME_LOW),
					row.getValueByColumn(COLUMN_NAME_CLOSE), row.getValueByColumn(COLUMN_NAME_VOLUME),
					row.getValueByColumn(COLUMN_NAME_SMA50), row.getValueByColumn(COLUMN_NAME_SMA200) };
			alertDataResult.add(values);
		}
		// check 50 SMA > 200 SMA and current volume 10% above average 50 day
		// volume
		if ((sma50 > 0) && (sma200 > 0) && (sma50 > sma200) && (currentVolume >= (1.1 * volume50))) {
			// create the data for bullish alert
			String[] values = new String[] { "bullish", row.getValueByColumn(COLUMN_NAME_TICKER),
					row.getValueByColumn(COLUMN_NAME_DATE), row.getValueByColumn(COLUMN_NAME_OPEN),
					row.getValueByColumn(COLUMN_NAME_HIGH), row.getValueByColumn(COLUMN_NAME_LOW),
					row.getValueByColumn(COLUMN_NAME_CLOSE), row.getValueByColumn(COLUMN_NAME_VOLUME),
					formatDoubleValue(volume50), row.getValueByColumn(COLUMN_NAME_SMA50),
					row.getValueByColumn(COLUMN_NAME_SMA200) };
			alertDataResult.add(values);
		}

	}

	/**
	 * generate headers following sentifi format
	 * 
	 * @return headers
	 */
	private List<String> generateHistoryHeaders() {
		List<String> headers = new ArrayList<String>();
		headers.add(COLUMN_NAME_TICKER);
		headers.add(COLUMN_NAME_DATE);
		headers.add(COLUMN_NAME_OPEN);
		headers.add(COLUMN_NAME_HIGH);
		headers.add(COLUMN_NAME_LOW);
		headers.add(COLUMN_NAME_CLOSE);
		headers.add(COLUMN_NAME_VOLUME);
		headers.add(COLUMN_NAME_TWAP_OPEN);
		headers.add(COLUMN_NAME_TWAP_HIGH);
		headers.add(COLUMN_NAME_TWAP_LOW);
		headers.add(COLUMN_NAME_TWAP_CLOSE);
		headers.add(COLUMN_NAME_SMA50);
		headers.add(COLUMN_NAME_SMA200);
		headers.add(COLUMN_NAME_LWMA15);
		headers.add(COLUMN_NAME_LWMA50);
		return headers;
	}

	/**
	 * connect to quandl url, read data, process, and print to csv file
	 * 
	 * @param quandlCode
	 */
	public void process(String quandlCode) {
		try {
			log.info("Start Connect to Quandl URL to get data");
			QuandlSession session = QuandlSession.create();
			DataResults data = session.getCsvResults(quandlCode);
			log.info("Finish Connect to Quandl URL to get data");
			log.info("Start process data from quandl");
			processQuandlData(quandlCode, data);
			log.info("Finish process data from quandl");
			
			log.info("Start print results to file");
			//make result folder
			makeDir();
			File csvFile = new File("Results" + File.separator + quandlCode + ".csv");
			File jsonFile = new File("Results" + File.separator + quandlCode + ".json");
			if (csvDataResult != null) {
				QuandlProcessUtils.printCSVToFile(csvDataResult, csvFile);
				log.info("Finish print csv file");
			}

			if (jsonDataResult != null && !jsonDataResult.isNull(COLUMN_NAME_PRICE)) {
				QuandlProcessUtils.printJSONObjectToFile(jsonDataResult, jsonFile);
				log.info("Finish print json file");
			}

			File alertFile = new File("Results" + File.separator + "alerts.dat");
			if (alertDataResult != null && !alertDataResult.isEmpty()) {
				QuandlProcessUtils.appendAlertToFile(alertDataResult, alertFile);
				log.info("Finish print alerts file");
			}
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	/**
	 * make Results folder to save results file
	 */
	public void makeDir() {
		Path path = Paths.get("Results");
        //if directory exists?
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                //fail to create directory
                try {
					throw e;
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
            }
        }
	}

	// private

}
