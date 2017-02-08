package com.sentifi.test.quandl.util;

import java.text.DecimalFormat;

public class NumberUtils {
	
	/**
	 * 
	 * @param strNum
	 * @return double value of strNum. if NumberFormatException throw, return 0D
	 */
	public static Double convertStringToDouble (String strNum) {
		try {
			return Double.parseDouble(strNum);
		} catch (NumberFormatException ex) {
			ex.printStackTrace();
			return 0D;
		}
	}
	
	/**
	 * format double value following #.####
	 * @param number
	 * @return string format of the number
	 */
	public static String formatDoubleValue (Double number) {
		DecimalFormat df = new DecimalFormat("#.#####");
		return df.format(number);
	}

}
