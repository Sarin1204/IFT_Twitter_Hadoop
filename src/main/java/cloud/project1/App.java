package cloud.project1;


import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Job j=new Job();
		j.setJarByClass(App.class);
		j.setJobName("Project");
		FileSystem fs= FileSystem.get(j.getConfiguration());
		FileStatus[] status_list = fs.listStatus(new Path("/project/input"));
		if(status_list != null){
		    for(FileStatus status : status_list){
		        //add each file to the list of inputs for the map-reduce job
		        FileInputFormat.addInputPath(j, status.getPath());
		    }
		}
		FileOutputFormat.setOutputPath(j,new Path("/project/output/tweetOutput1"));
		j.setMapperClass(Mapper1.class);
		j.setReducerClass(Reducer1.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(CustomWritable.class);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}
}
