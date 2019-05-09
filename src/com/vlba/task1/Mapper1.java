package com.vlba.task1;

import java.io.IOException;
import java.util.regex.Matcher;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.vlba.src.MovieDataAnalyser;

/*
 * Mapper class for Task 1
 * Spliting the csv lines with comma delimiter and String delimited
 */
public class Mapper1 extends Mapper<Object, Text, IntWritable, Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		/*
		 * Delimiting the line using \t escape sequence, but *IMPORTANT: There are text in the
		 * same line which is a string and a String can contain multiple commas and we
		 * do not want to split that string but want to count that as a one single
		 * String. Hence adding a condition in the Regular expression
		 **/
		String[] tokens = value.toString().split("\t(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

		// If Genre contains "Music" string in it then we need this result
		if (tokens[2].indexOf("Music") != -1) {
			/*
			 * Release date column in the CSV file can have any charater in it, hence
			 * Regular expression for 2 date formats are created.
			 * Format 1 -> dd.MM.yyyy
			 * Format 2 -> dd/MM/yyyy
			 */
			Matcher dateMatcher = MovieDataAnalyser.DATE_PATTERN.matcher(tokens[10]);
			if (dateMatcher.find()) {
				// Getting the Year of the release date
				Matcher yearMatcher = MovieDataAnalyser.YEAR_PATTERN.matcher(dateMatcher.group());
				if (yearMatcher.find()) {
					int year = Integer.parseInt(yearMatcher.group());
					// Write to context only if the row has the year greater than 1980
					if (year >= 1980) {
						context.write(new IntWritable(year), new Text(tokens[5]));
					}
				}
			}
		}
	}
}
