package com.vlba.src;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.vlba.task1.Reducer1;
import com.vlba.task1.Mapper1;
import com.vlba.task2.DataTask2;
import com.vlba.task2.Reducer2;
import com.vlba.task2.Mapper2;
import com.vlba.task3.DataTask3;
import com.vlba.task3.Reducer3;
import com.vlba.task3.Mapper3;

public class MovieDataAnalyser {

	/*
	 * Global static values for all Tokenziers and Reducers
	 */
	public static final Pattern DATE_PATTERN = Pattern
			.compile("(\\d{1,2}\\.\\d{1,2}\\.\\d{4})|(\\d{1,2}/\\d{1,2}/\\d{4})");
	public static final Pattern YEAR_PATTERN = Pattern.compile("\\d{4}");

	/*
	 * args[] =>
	 * args[0] -> Task number to run. Could be (1, 2, 3) only
	 * args[1] -> Input file. (the movie csv file)
	 * args[2] -> Output directory to which you want to store the result
	 * Task 2-> args[3] -> imdb_id
	 * args[4] -> release_date
	 * Task 3-> args[3] -> Term 1
	 * args[4] -> Term 2
	 * args[5] -> Term 3
	 */
	public static void main(String[] args) throws Exception {
		/*
		 * Checking for correct parameters
		 */
		if (args.length < 3) {
			System.err.println("Please provide valid arguments: Task_Number(1,2,3) Inputfile(csv) outputFolder");
			System.exit(1);
		}
		if (!args[0].equals("1") && !args[0].equals("2") && !args[0].equals("3")) {
			System.err.println("Invalid Task number. Valid arguments: Task_Number(1,2,3) Inputfile(csv) outputFolder");
			System.exit(1);
		}
		if (args[0].equals("1") && args.length != 3) {
			System.err
					.println("Please provide valid arguments for Task 1 :- Task_Number(1) Inputfile(csv) outputFolder");
			System.exit(1);
		}
		if (args[0].equals("2") && args.length != 5) {
			System.err.println(
					"Please provide valid arguments for Task 2 :- Task_Number(2) Inputfile(csv) outputFolder imdb_id ReleaseDate(dd.MM.yyyy)");
			System.exit(1);
		}
		if (args[0].equals("3") && (args.length < 4 || args.length > 6)) {
			System.err.println(
					"Please provide valid arguments for Task 3 :- Task_Number(3) Inputfile(csv) outputFolder Keyword1 Keyword2 Keyword3");
			System.exit(1);
		}

		/*
		 * Making a Hadoop job and configuration
		 */
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "Task :" + args[0]);
		job.setJarByClass(MovieDataAnalyser.class);
		switch (args[0]) {
		case "1":
			job.setMapperClass(Mapper1.class);
			job.setReducerClass(Reducer1.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			break;
		case "2":
			// Checking if the date entered by user is correct
			Matcher dateMatcher = Pattern.compile("^\\d{1,2}\\.\\d{1,2}\\.\\d{4}$").matcher(args[4]);
			if (!dateMatcher.find()) {
				System.err.println("Please provide valid date format");
				System.exit(1);
			}
			// Checking if the imdb_id entered by user is an Integer
			try {
				DataTask2.imdb_id = Integer.parseInt(args[3]);
			} catch (NumberFormatException e) {
				System.err.println("Imdb_id must be an Integer. Please try again");
				System.exit(1);
			}
			// Using DataTask2 class to hold input release date and imdb id for further
			// MapReduce
			DataTask2.date = args[4];
			job.setMapperClass(Mapper2.class);
			job.setReducerClass(Reducer2.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			break;
		case "3":
			// Using DataTask3 class to hold all the terms provided by the user in the Map.
			for (int i = 3; i < args.length; i++) {
				DataTask3.words.put(args[i].toLowerCase(), 0);
			}
			job.setMapperClass(Mapper3.class);
			job.setReducerClass(Reducer3.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			break;
		}

		Path inputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		// Path for holding the result file
		Path resultCopy = new Path(args[2] + "/part-r-00000");
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		if (job.waitForCompletion(true)) {
			/*
			 * Creating a Folder named RESULTS_123 for keeping the result file
			 * "part-r-00000" of the ran output folder, and rename the file to Task1 Task2
			 * or Task3 .csv
			 */
			File file = new File("RESULTS_123");
			if (!file.exists()) {
				file.mkdir();
			}
			Path output = new Path("RESULTS_123/Task" + args[0] + ".csv");
			hdfs.delete(output, true);
			hdfs.copyFromLocalFile(resultCopy, output);
		}
		System.exit(0);
	}
}
