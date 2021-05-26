package edu.kul.dtai.bdap;

import java.util.regex.Pattern;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Helper class for "Writable" classes.
 * 
 * @author Nicomak
 *
 */
public class StringHelper {

	private static Pattern quotes = Pattern.compile("'");
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	private static TimeZone timeZoneLA = TimeZone.getTimeZone("America/Los_Angeles");

	/**
	 * Removes quote characters from a string.
	 * 
	 * @param csvField
	 * @return
	 */
	public static String removeSingleQuotes(String text) {
		return quotes.matcher(text).replaceAll("");
	}

	/**
	 * Parses a string into a double. Returns 0 if the value cannot be parsed.
	 * 
	 * @param csvField
	 * @return
	 */
	public static double parseDouble(String csvField) {
		try {
			return Double.parseDouble(csvField);
		} catch (Exception e) {
			return 0d;
		}
	}

	/**
	 * Parses a string into an int. Returns 0 if the value cannot be parsed.
	 * 
	 * @param csvField
	 * @return
	 */
	public static int parseInt(String csvField) {
		try {
			return Integer.parseInt(csvField);
		} catch (Exception e) {
			return 0;
		}
	}

	/**
	 * Parses a "E" (false) or "M" (true) valued string into a boolean. Returns
	 * false if value is different than "M".
	 * 
	 * @param csvField
	 * @return
	 */
	public static boolean parseStatus(String csvField) {
		if ("M".equals(csvField)) {
			return true;
		}
		return false;
	}

	/**
	 * Parses a date string formatted as 'YYYY-MM-DD HH:MM::SS' to a timestamp of
	 * type Long. Returns 0 if the value cannot be parsed.
	 * 
	 * @param csvField
	 * @return
	 */
	public static Long parseDate(String csvField) {
		try {
			dateFormat.setTimeZone(timeZoneLA);
			Date parsedDate = dateFormat.parse(csvField);
			return parsedDate.getTime();
		} catch (Exception e) {
			System.out.println(e);
			return 0l;
		}
	}
}