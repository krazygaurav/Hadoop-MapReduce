package com.vlba.task3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reducer class for Task 3
 * Reduces the value simply by adding all the values for one key which in this
 * case will always be 1 and hence the loop will execute only once for each key
 */
public class Reducer3
		extends Reducer<IntWritable, Text, IntWritable, Text> {

	public void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		String value = "";
		for (Text text : values) {
			value += text.toString();
		}
		if (!value.equals(""))
			context.write(key, new Text(value));
	}
}