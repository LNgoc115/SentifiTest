package com.sentifi.test.quandl.main;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.sentifi.test.quandl.processor.QuandlProcessor;

public class App {
	
	public static final String TICKER_KEY = "ticker";
	
	public static Logger log = Logger.getLogger(App.class);

	public static void main(String[] args) {
		
		PropertyConfigurator.configure("../etc/log.conf");
		log.info("Finish Load log.conf");

		/***********Read quandl code from config***********/
		Properties prop = new Properties();
		InputStream input = null;
		String quandlCode = "";

		try {

			input = new FileInputStream("../etc/config.properties");

			// load a properties file
			prop.load(input);
			
			quandlCode = prop.getProperty(TICKER_KEY);
			log.info("Finish read quandl code from config.properties");

			// get the property value and print it out

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		/*************End read quandl code ****************/
		if (!quandlCode.isEmpty()) {
			//process with quandl code
			new QuandlProcessor().process(quandlCode);
			log.info("Finish APP");
		} else {
			log.error("No ticker found in config.properties");
		}
		
	}

}
