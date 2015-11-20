package cloud.project1;

import java.io.*;
import java.util.*;

import cloud.project1.CustomWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import twitter4j.Status;

public class Reducer1 extends Reducer<Text,CustomWritable,Text,Text>{

	@Override
	public void reduce(Text arg0, Iterable<CustomWritable> arg1,
			Context context) throws IOException, InterruptedException {

		HashMap<String,String> place=new HashMap<String,String>();
		HashMap<String,Integer> plcount1=new HashMap<String,Integer>();
		ValueComparator vcm=new ValueComparator(plcount1);
		TreeMap<String,Integer> plcount2=new TreeMap<String,Integer>(vcm);
//		List<Integer> countl=new ArrayList<Integer>();
		ArrayList<Text> outlist=new ArrayList<Text>();
		int count=0,total=0;
		int follower=0,placecount=0;
		for(CustomWritable s:arg1)
		{
			try{
					System.out.println("Inside Reducer2");
					follower=follower+s.getValue().get();
					count++;
					String plc=s.getPlace().toString();
					if(plcount1.containsKey(plc))
					{
						placecount=plcount1.get(plc)+1;
						plcount1.put(plc,placecount);
					}
					else
					{
						plcount1.put(plc, 1);
					}
							
				}
			catch(Exception e)
			{
				System.err.print(e.getMessage());
				System.out.println("Inside Reducer1 exception"+e);
			}
		}
		
		int fol=follower/count;
		String strfol=Integer.toString(fol);
		String strcnt=Integer.toString(count);
		outlist.add(new Text(strfol));
		outlist.add(new Text(strcnt));
		plcount2.putAll(plcount1);
		System.out.println("Place map"+plcount2);
		for(int i=0;i<10;i++)
		{
			Map.Entry<String, Integer> ent=plcount2.pollFirstEntry();			
			outlist.add(new Text(ent.getKey()+" "+ent.getValue()));
		}
	
		String out0="Total tweets about " +arg0.toString();
		String out1="Average Influence of people who tweeted about "+arg0.toString();
//		TextArrayWritable taw=new TextArrayWritable(Text.class,outlist.toArray());
		context.write(new Text(out0), new Text(outlist.get(0)));
		context.write(new Text(out1), new Text(outlist.get(1)));
		String out2="Tweet Count for " +arg0.toString()+" at ";
		for(int i=2;i<12;i++)
		{
		context.write(new Text(out2),new Text(outlist.get(i)));	
		}
	}
		
}
