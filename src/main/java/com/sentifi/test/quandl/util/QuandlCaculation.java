package com.sentifi.test.quandl.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import static com.sentifi.test.quandl.util.NumberUtils.*;
import static com.sentifi.test.quandl.util.Constant.*;

import com.sentifi.test.quandl.connection.Row;

/**
 * Calculate TWA, SMA, LWMA, average volume...
 * 
 * @author NGOCLM
 *
 */
public class QuandlCaculation {

	// contain 50 last close price to calculate 50 day sma
	private Queue<Double> sma50Queue = new LinkedList<Double>();

	// contain 200 last close price to calculate 200 day sma
	private Queue<Double> sma200Queue = new LinkedList<Double>();

	// contain 15 last close price to calculate 15 day lwma
	private Queue<Double> lwma15Queue = new LinkedList<Double>();

	// contain 50 last close price to calculate 50 day lwma
	private Queue<Double> lwma50Queue = new LinkedList<Double>();
	
	// contain 50 last volume to calculate 50 day average volume
	private Queue<Double> volume50Queue = new LinkedList<Double>();

	//sum of 50 day close price
	private Double sma50Sum = 0D;
	//sum of 200 day close price
	private Double sma200Sum = 0D;
	//sum of 15 day weighted close price
	private Double lwma15Sum = 0D;
	//sum of 50 day weighted close price
	private Double lwma50Sum = 0D;
	//sum of open price
	private Double twaOpenSum = 0D;
	//sum of high price
	private Double twaHighSum = 0D;
	//sum of low price
	private Double twaLowSum = 0D;
	//sum of close price
	private Double twaCloseSum = 0D;
	// number of price to calculate twa
	private Integer twaNum = 0; 
	//Sum of 50 day volume
	private Double volume50Sum = 0D;
	
	public void init(List<Row> rawRows) {
		initSMA50Queue(rawRows);
		initSMA200Queue(rawRows);
		initLWMA15Queue(rawRows);
		initLWMA50Queue(rawRows);
		initTWA(rawRows);
		initVolume50Queue(rawRows);
	}
		
	
	/**
	 * get first 50 volume to add to queue
	 * 
	 * @param rawRows
	 */
	public void initVolume50Queue(List<Row> rawRows) {
		List<Row> subListVolume50 = new ArrayList<Row>();
		if (rawRows != null && rawRows.size() >= VOLUME50_LENGTH) {
			subListVolume50 = rawRows.subList(0, VOLUME50_LENGTH - 1);
		}
		this.addListRowToVolume50Queue(subListVolume50);
	}
	
	/**
	 * add list volume to volume50queue
	 * 
	 * @param list
	 */
	public void addListRowToVolume50Queue(List<Row> list) {
		if (list != null) {
			for (Row row : list) {
				Double number = convertStringToDouble(row.getValueByColumn(COLUMN_NAME_VOLUME));
				addNumberToVolume50Queue(number);
			}
		}
	}
	
	/**
	 * add number to volume50 queue, calculate again volume50num: plus new number and
	 * minus the first number of queue if size of queue > 50
	 * 
	 * @param number
	 */
	public void addNumberToVolume50Queue(Double number) {
		volume50Sum += number;
		volume50Queue.add(number);
		if (volume50Queue.size() > VOLUME50_LENGTH) {
			// re calculate volume50Sum, and remove the first number in queue
			volume50Sum -= volume50Queue.remove();
		}
	}
		
	/**
	 * calculate sum of all price for each: open, high, low, clos over the ranage
	 * @param rawRows
	 */
	public void initTWA(List<Row> rawRows) {
		if (rawRows != null) {
			for (Row row : rawRows) {
				twaOpenSum += convertStringToDouble(row.getValueByColumn(COLUMN_NAME_OPEN));
				twaHighSum += convertStringToDouble(row.getValueByColumn(COLUMN_NAME_HIGH));
				twaLowSum += convertStringToDouble(row.getValueByColumn(COLUMN_NAME_LOW));
				twaCloseSum += convertStringToDouble(row.getValueByColumn(COLUMN_NAME_CLOSE));
			}
			twaNum = rawRows.size();
		}
	}
	
	/**
	 * calculate twa open value at a point
	 * @param row
	 * @return  twa open value at a point
	 */
	public Double calculateTWAOpen(Row row) {
		Double twaOpen = twaOpenSum / twaNum;
		twaOpenSum -= convertStringToDouble(row.getValueByColumn(COLUMN_NAME_OPEN));
		return twaOpen;
	}
	
	/**
	 * calculate twa high value at a point
	 * @param row
	 * @return  twa high value at a point
	 */
	public Double calculateTWAHigh(Row row) {
		Double twaHigh = twaHighSum / twaNum;
		twaHighSum -= convertStringToDouble(row.getValueByColumn(COLUMN_NAME_HIGH));
		return twaHigh;
	}
	
	/**
	 * calculate twa low value at a point
	 * @param row
	 * @return  twa low value at a point
	 */
	public Double calculateTWALow(Row row) {
		Double twaLow = twaLowSum / twaNum;
		twaLowSum -= convertStringToDouble(row.getValueByColumn(COLUMN_NAME_LOW));
		return twaLow;
	}
	
	/**
	 * calculate twa close value at a point
	 * @param row
	 * @return  twa close value at a point
	 */
	public Double calculateTWAClose(Row row) {
		Double twaClose = twaCloseSum / twaNum;
		twaCloseSum -= convertStringToDouble(row.getValueByColumn(COLUMN_NAME_CLOSE));
		return twaClose;
	}
	
	/**
	 * after calculate a row, minus twaNum 1
	 */
	public void minusTWANum() {
		twaNum --;
	}
	
	
	/**
	 * get first 49 close price to add to queue
	 * 
	 * @param rawRows
	 */
	public void initSMA50Queue(List<Row> rawRows) {
		List<Row> subListSma50 = new ArrayList<Row>();
		if (rawRows != null && rawRows.size() >= SMA50_LENGTH) {
			subListSma50 = rawRows.subList(0, SMA50_LENGTH - 1);
		}
		this.addListRowToSMA50Queue(subListSma50);
	}

	/**
	 * get first 199 close price to add to queue
	 * 
	 * @param rawRows
	 */
	public void initSMA200Queue(List<Row> rawRows) {
		List<Row> subListSma200 = new ArrayList<Row>();
		if (rawRows != null && rawRows.size() >= SMA200_LENGTH) {
			subListSma200 = rawRows.subList(0, SMA200_LENGTH - 1);
		}
		this.addListRowToSMA200Queue(subListSma200);
	}

	/**
	 * get first 49 close price to add to queue
	 * 
	 * @param rawRows
	 */
	public void initLWMA50Queue(List<Row> rawRows) {
		List<Row> subListLwma50 = new ArrayList<Row>();
		if (rawRows != null && rawRows.size() >= LWMA50_LENGTH) {
			subListLwma50 = rawRows.subList(0, LWMA50_LENGTH - 1);
		}
		this.addListRowToLWMA50Queue(subListLwma50);
	}
	
	/**
	 * get first 14 close price to add to queue
	 * 
	 * @param rawRows
	 */
	public void initLWMA15Queue(List<Row> rawRows) {
		List<Row> subListLwma15 = new ArrayList<Row>();
		if (rawRows != null && rawRows.size() >= LWMA15_LENGTH) {
			subListLwma15 = rawRows.subList(0, LWMA15_LENGTH - 1);
		}
		this.addListRowToLWMA15Queue(subListLwma15);
	}

	/**
	 * add list close price to lwma 15 queue
	 * 
	 * @param list
	 */
	public void addListRowToLWMA15Queue(List<Row> list) {
		if (list != null) {
			for (Row row : list) {
				Double number = convertStringToDouble(row.getValueByColumn(COLUMN_NAME_CLOSE));
				addNumberToLWMA15Queue(number);
			}
		}
	}
	
	/**
	 * add list close price to lwma 50 queue
	 * 
	 * @param list
	 */
	public void addListRowToLWMA50Queue(List<Row> list) {
		if (list != null) {
			for (Row row : list) {
				Double number = convertStringToDouble(row.getValueByColumn(COLUMN_NAME_CLOSE));
				addNumberToLWMA50Queue(number);
			}
		}
	}

	/**
	 * add number to lwma15 queue, calculate again lwma15sum: 
	 * re calculate the lwma15 sum if the queue size > 15
	 * 
	 * @param number
	 */
	public void addNumberToLWMA15Queue(Double number) {
		if (lwma15Queue.size() < LWMA15_LENGTH) {
			lwma15Sum += number * (LWMA15_LENGTH - lwma15Queue.size());
		}
		lwma15Queue.add(number);
		if (lwma15Queue.size() > LWMA15_LENGTH) {
			//  remove the first number in queue
			lwma15Sum -= (lwma15Queue.remove() * LWMA15_LENGTH);
			//re calculate lwma15Sum
			Iterator<Double> iterLwma15 = lwma15Queue.iterator();
			while (iterLwma15.hasNext()) {
				lwma15Sum += iterLwma15.next();
			}
		}
	}
	
	/**
	 * add number to lwma50 queue, calculate again lwma50sum:
	 *  re calculate the lwma50 sum if the queue size > 50
	 * 
	 * @param number
	 */
	public void addNumberToLWMA50Queue(Double number) {
		if (lwma50Queue.size() < LWMA50_LENGTH) {
			lwma50Sum += number * (LWMA50_LENGTH - lwma50Queue.size());
		}
		lwma50Queue.add(number);
		if (lwma50Queue.size() > LWMA50_LENGTH) {
			//  remove the first number in queue
			lwma50Sum -= (lwma50Queue.remove() * LWMA50_LENGTH);
			//re calculate lwma50Sum
			Iterator<Double> iterLwma50 = lwma50Queue.iterator();
			while (iterLwma50.hasNext()) {
				lwma50Sum += iterLwma50.next();
			}
		}
	}

	/**
	 * add list close price to sma 50 queue
	 * 
	 * @param list
	 */
	public void addListRowToSMA50Queue(List<Row> list) {
		if (list != null) {
			for (Row row : list) {
				Double number = convertStringToDouble(row.getValueByColumn(COLUMN_NAME_CLOSE));
				addNumberToSMA50Queue(number);
			}
		}
	}

	/**
	 * add list close price to sma 200 queue
	 * 
	 * @param list
	 */
	public void addListRowToSMA200Queue(List<Row> list) {
		if (list != null) {
			for (Row row : list) {
				Double number = convertStringToDouble(row.getValueByColumn(COLUMN_NAME_CLOSE));
				addNumberToSMA200Queue(number);
			}
		}
	}

	/**
	 * add number to sma50 queue, calculate again sma50sum: plus new number and
	 * minus the first number of queue if size of queue > 50
	 * 
	 * @param number
	 */
	public void addNumberToSMA50Queue(Double number) {
		sma50Sum += number;
		sma50Queue.add(number);
		if (sma50Queue.size() > SMA50_LENGTH) {
			// re calculate sma50Sum, and remove the first number in queue
			sma50Sum -= sma50Queue.remove();
		}
	}

	/**
	 * calculate the sma 50 value of a point
	 * 
	 * @return the sma 50 value of the point. If the size of queue not equal 50,
	 *         return 0D
	 */
	public Double calculateSMA50() {
		if (sma50Queue.size() == SMA50_LENGTH)
			return sma50Sum / SMA50_LENGTH;
		return 0D;
	}

	/**
	 * add number to sma200 queue, calculate again sma50sum: plus new number and
	 * minus the first number of queue if size of queue > 200
	 * 
	 * @param number
	 */
	public void addNumberToSMA200Queue(Double number) {
		sma200Sum += number;
		sma200Queue.add(number);
		if (sma200Queue.size() > SMA200_LENGTH) {
			// re calculate sma200Sum, and remove the first number in queue
			sma200Sum -= sma200Queue.remove();
		}
	}

	/**
	 * calculate the sma 200 value of a point
	 * 
	 * @return the sma 200 value of the point. If the size of queue not equal
	 *         200, return 0D
	 */
	public Double calculateSMA200() {
		if (sma200Queue.size() == SMA200_LENGTH)
			return sma200Sum / SMA200_LENGTH;
		return 0D;
	}
	
	/**
	 * calculate the average 50 day volume of a point
	 * 
	 * @return the average 50 day volume of a point. If the size of queue not equal
	 *         50, return 0D
	 */
	public Double calculateVolume50() {
		if (volume50Queue.size() == VOLUME50_LENGTH)
			return volume50Sum / VOLUME50_LENGTH;
		return 0D;
	}
	
	/**
	 * calculate the lwma15 value of a point
	 * 
	 * @return the lwma 15 value of the point. If the size of queue not equal 15,
	 *         return 0D
	 */
	public Double calculateLWMA15() {
		if (lwma15Queue.size() == LWMA15_LENGTH)
			return lwma15Sum / (LWMA15_LENGTH * (LWMA15_LENGTH + 1) / 2);
		return 0D;
	}
	
	/**
	 * calculate the lwma 50 value of a point
	 * 
	 * @return the lwma 50 value of the point. If the size of queue not equal 50,
	 *         return 0D
	 */
	public Double calculateLWMA50() {
		if (lwma50Queue.size() == LWMA50_LENGTH)
			return lwma50Sum / (LWMA50_LENGTH * (LWMA50_LENGTH + 1) / 2);
		return 0D;
	}

}
