package com.vlba.task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reducer class for Task 2
 * Checks if the duration of the film (of given imdb_id) is greater than or less
 * than duration +-10
 */
public class Reducer2
		extends Reducer<IntWritable, Text, IntWritable, Text> {

	public void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		int allowedDuration = DataTask2.duration;
		String value = "";
		int duration = -1;
		for (Text text : values) {
			String tokens[] = text.toString().split("\\s+");
			try {
				duration = Integer.parseInt(tokens[0]);
			} catch (NumberFormatException e) {
			}
			/*
			 * If duration is -1 means that the row has some parsing issues or contains the
			 * invalid data.
			 * If DataTask2.duration == -1 -> We didn't found the ID in the CSV data and hence no writing in the context
			 */ 
			if (DataTask2.duration!=-1 && duration != -1 && duration >= allowedDuration - 10 && duration <= allowedDuration + 10) {
				value += text.toString();
			}
		}
		/*
		 * If value contains something which means it satisfies the condition of allowed
		 * duration. Then only write to the Context
		 */
		if (!value.equals(""))
			context.write(key, new Text(value));
	}
}