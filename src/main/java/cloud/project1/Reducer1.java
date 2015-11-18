package cloud.project1;

import java.io.*;
import java.util.*;

import cloud.project1.TextArrayWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import twitter4j.Status;

public class Reducer1 extends Reducer<Text,TextArrayWritable,Text,Text>{

	@Override
	public void reduce(Text arg0, Iterable<TextArrayWritable> arg1,
			Context context) throws IOException, InterruptedException {
		int count=0;
		Text t1[]=new Text[0];
		for(TextArrayWritable s:arg1)
		{
			try{
					System.out.println("Inside Reducer1");
					context.write(arg0, s.getValue());
			}
			catch(Exception e)
			{
				System.err.print(e.getMessage());
				System.out.println("Inside Reducer1 exception"+e);
			}
		
		}
	}
		
}
