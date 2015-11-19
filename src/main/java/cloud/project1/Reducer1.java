package cloud.project1;

import java.io.*;
import java.util.*;

import cloud.project1.CustomWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import twitter4j.Status;

public class Reducer1 extends Reducer<Text,CustomWritable,Text,IntWritable>{

	HashMap<String,String> place=new HashMap<String,String>();
	HashMap<String,Integer> plcount=new HashMap<String,Integer>();
	@Override
	public void reduce(Text arg0, Iterable<CustomWritable> arg1,
			Context context) throws IOException, InterruptedException {
		int count=0,total=0;
		int follower=0,placecount=0;
		for(CustomWritable s:arg1)
		{
			try{
					System.out.println("Inside Reducer1");
					follower=follower+s.getValue().get();
					count++;
/*					String plc=s.getPlace().toString();
					{
					if(place.containsKey(plc))
						placecount=placecount+1;
						plcount.put(plc, placecount);
					}
					else
					{
						place.put(plc, arg0.toString());
					}*/
							
				}
			catch(Exception e)
			{
				System.err.print(e.getMessage());
				System.out.println("Inside Reducer1 exception"+e);
			}
		}
		
		int fol=follower/count;
		String out0="Total tweets about " +arg0.toString();
		context.write(new Text(out0), new IntWritable(count));
		String out1="Average Influence of people who tweeted about "+arg0.toString();
		context.write(new Text(out1),new IntWritable(fol));
		

	}
		
}
