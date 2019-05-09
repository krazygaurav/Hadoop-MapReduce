package com.vlba.task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reducer class for Task 1
 * Counts the occurance of all years and put a total of it along with the movies
 * seperated by the Tab delimiter
 */
public class Reducer1
		extends Reducer<IntWritable, Text, IntWritable, Text> {

	public void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		int total = 0;
		String value = "";
		// Keeping the Movies with same year by a tab seperated delimiter.
		for (Text text : values) {
			value += text.toString() + "\t";
			total++;
		}
		/*
		 * I dont have to check for null conditions on substring as this process will
		 * occur only if we got instances of movies in the file.
		 */
		value = total + "\t" + value.substring(0, value.length() - 1);
		context.write(key, new Text(value));
	}
}