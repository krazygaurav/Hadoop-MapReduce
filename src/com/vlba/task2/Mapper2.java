package com.vlba.task2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.vlba.src.MovieDataAnalyser;

/*
 * Mapper class for Task 2
 * Tasks performed by the Class
 * 1. Parses the File and find the Duration for the film with given imdb_id and
 * saves it to the Data File for the task
 * 2. Map only movies which are released after the release date provided in the
 * arguments
 */
public class Mapper2 extends Mapper<Object, Text, IntWritable, Text> {
	SimpleDateFormat format = new SimpleDateFormat("dd.MM.yyyy");

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Date startDate = null;
		try {
			startDate = format.parse(DataTask2.date);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}

		/*
		 * Delimiting the line using \t escape sequence, but *IMPORTANT: There are text in the
		 * same line which is a string and a String can contain multiple commas and we
		 * do not want to split that string but want to count that as a one single
		 * String. Hence adding a condition in the Regular expression
		 **/
		String tokens[] = value.toString().split("\t(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		int duration = -1;
		int id = -1;
		try {
			id = Integer.parseInt(tokens[0]);
			duration = Integer.parseInt(tokens[12]);
			/* Holds the duration of the movie with provided imdb_id */
			if (id == DataTask2.imdb_id) {
				DataTask2.duration = duration;
			}
		} catch (NumberFormatException e) {
		}

		/*
		 * Release date column in the CSV file can have any charater in it, hence
		 * Regular expression for 2 date formats are created.
		 * Format 1 -> dd.MM.yyyy
		 * Format 2 -> dd/MM/yyyy
		 */
		Matcher dateMatcher = MovieDataAnalyser.DATE_PATTERN.matcher(tokens[10]);
		if (dateMatcher.find()) {
			String date = dateMatcher.group();
			/*
			 * Converting the dates with format dd/MM/yyyy to dd.MM.yyyy for simplifying the
			 * process
			 */
			date.replace("/", ".");
			try {
				Date releaseDate = format.parse(date);
				/*
				 * If the release date of the movie is afte the provided release date, then only
				 * put the data into the context
				 */
				if (null != startDate && releaseDate.after(startDate)) {
					context.write(new IntWritable(id), new Text(duration + "\t" + tokens[5] + "\t" + date));
				}
			} catch (ParseException e) {
			}
		}
	}
}
