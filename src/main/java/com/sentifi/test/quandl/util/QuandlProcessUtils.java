package com.sentifi.test.quandl.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.opencsv.CSVWriter;
import com.sentifi.test.quandl.connection.DataResults;
import com.sentifi.test.quandl.connection.Row;

/**
 * 
 * @author NGOCLM
 *
 */
public class QuandlProcessUtils {
	
	/**
	 * print json object to file .json
	 * 
	 * @param object
	 * @param file
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public static void printJSONObjectToFile(JSONObject object, File file) throws JsonParseException, JsonMappingException, IOException {
		ArgumentChecker.notNull(object, "Json Object");
		ArgumentChecker.notNull(file, "File");
		ObjectMapper mapper = new ObjectMapper();
		Object json = mapper.readValue(object.toString(), Object.class);
		ObjectWriter writer = mapper.writer(new DefaultPrettyPrinter());
		writer.writeValue(file,json);
	}
	
	/**
	 * print csv object to file .csv
	 * @param csv
	 * @param file
	 * @throws IOException 
	 */
	public static void printCSVToFile(DataResults csv, File file) throws IOException {
		ArgumentChecker.notNull(csv, "csv object");
		ArgumentChecker.notNull(file, "File");
		CSVWriter writer = new CSVWriter(new FileWriter(file),',','"');
		//print header of csv
		writer.writeNext(csv.getHeaders().toArray(new String[0]));
		//print data of csv
		for (Row row : csv.getRows()) {
			writer.writeNext(row.getValues());
		}
		writer.close();
	}
	
	/**
	 * append alert data to file 
	 * @param alertData
	 * @param file
	 * @throws IOException 
	 */
	public static void appendAlertToFile(List<String[]> alertData, File file) throws IOException {
		ArgumentChecker.notNull(alertData, "alertData");
		ArgumentChecker.notNull(file, "File");
		CSVWriter writer = new CSVWriter(new FileWriter(file, true),',','"');
		//print data of alertData
		for (String[] values : alertData) {
			writer.writeNext(values);
		}
		writer.close();
	}

}
