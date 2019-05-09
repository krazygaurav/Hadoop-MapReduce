package com.vlba.task3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper class for Taks 3
 * Tasks:
 * 1. Creates a local map for all the Rows in the CSV file and get itself
 * initialized with the Term values provided from command line arguments
 * 2.Splits the Overview column and stores the frequency of terms in the Map and
 * write those <key,value> (term, frequency) in the Context with respect to
 * movie id
 */
public class Mapper3 extends Mapper<Object, Text, IntWritable, Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		/* Local map to hold term and its frequency */
		Map<String, Integer> words = new HashMap<>();
		/*
		 * Initializing the local map with the term map with 0 frequency for each term
		 */
		words.putAll(DataTask3.words);
		
		/*
		 * Delimiting the line using \t escape seq, but *IMPORTANT: There are text in the
		 * same line which is a string and a String can contain multiple commas and we
		 * do not want to split that string but want to count that as a one single
		 * String. Hence adding a condition in the Regular expression
		 **/
		String[] tokens = value.toString().split("\t(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		String overview = tokens[6];
		String overviewWords[] = overview.split("\\s+");
		
		boolean found = false;
		for (String word : overviewWords) {
			word = word.toLowerCase();
			if (DataTask3.words.containsKey(word)) {
				words.put(word, words.get(word) + 1);
				/*
				 * If found == true, means a movie has atleast one required keyword and hence
				 * needs to be in the Context otherwise reject the data row
				 */
				found = true;
			}
		}
		try {
			if (found)
				context.write(new IntWritable(Integer.parseInt(tokens[0])), new Text(words.toString()));
		} catch (NumberFormatException e) {
			System.err.println("Id should be Integer");
		}
	}
}
