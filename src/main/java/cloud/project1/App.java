package cloud.project1;


import java.util.*;
import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class App {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Job j=new Job();
		j.setJarByClass(App.class);
		j.setJobName("Project");
		FileInputFormat.addInputPath(j,new Path("/user/flume/tweets/FlumeData.1447173827757"));
		FileOutputFormat.setOutputPath(j,new Path("/user/mapred/output"));
		j.setMapperClass(Mapper1.class);
		j.setReducerClass(Reducer1.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(TextArrayWritable.class);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}
}
