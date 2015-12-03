package cloud.project1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cloud.project1.CustomWritable;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;


public class Mapper1 extends Mapper<LongWritable,Text,Text,CustomWritable>{
	Logger log= LoggerFactory.getLogger(Mapper1.class);
	CustomWritable tw =new CustomWritable();
	int tempcount=0;
	HashMap<String,String> input=new HashMap<String,String>();
	
	@Override
	public void setup(Context ct)
	{
			input.put("Hillary", "Hillary Clinton");
			input.put("Clinton", "Hillary Clinton");
			input.put("@HillaryClinton", "Hillary Clinton");
			input.put("Ben", "Ben Carson");
			input.put("#HillaryClinton", "Hillary Clinton");
			input.put("Carson", "Ben Carson");
			input.put("@RealBenCarson", "Ben Carson");
			input.put("#BenCarson","Ben Carson");
			input.put("Jeb","Jeb Bush");
			input.put("Bush","Jeb Bush");
			input.put("@JebBush", "Jeb Bush");
			input.put("#JebBush","Jeb Bush");
			input.put("Bernie", "Bernie Sander");
			input.put("Sanders", "Bernie Sander");
			input.put("#BernieSanders","Bernie Sander");
			input.put("#MartinO'Malley", "Martin O'Malley");
			input.put("O'Malley", "Martin O'Malley");
			input.put("@GovernorOMalley","Martin O'Malley");
			input.put("Ted", "Ted Cruz");
			input.put("Cruz", "Ted Cruz");
			input.put("@tedcruz", "Ted Cruz");
			input.put("Donald J.","Donald Trump");
			input.put("Trump", "Donald Trump");
			input.put("#DonaldTrump","Donald Trump");
			input.put("@realDonaldTrump", "Donald Trump");
		
	}
	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		try {
			HashMap<Text,CustomWritable> output=new HashMap<Text,CustomWritable>();
			Status s;
			s = TwitterObjectFactory.createStatus(value.toString());
			/*System.out.println("After create status");*/
			String status=s.getText();
			/*System.out.println("After status getText "+status);*/
			StringTokenizer st=new StringTokenizer(status," ");
			while(st.hasMoreTokens())
			{
			String in1=st.nextToken();	
			if(input.containsKey(in1))
			{
				
				int fol=s.getUser().getFollowersCount();
				String place=s.getUser().getLocation();
				int retweet = s.getRetweetedStatus().getRetweetCount();
				Text tplace=new Text(place);
				Text candidate=new Text(input.get(in1));
				tw.set(new IntWritable(fol),tplace,new IntWritable(retweet));
				output.put(candidate,tw);
			}
			}
			for(Map.Entry<Text,CustomWritable> input:output.entrySet())
			{
				context.write(input.getKey(), input.getValue());
			}
			
		} catch (Exception e) {			
			/*System.err.println("Inside TwitterException for tweet value"+value.toString());*/
			return;
		}
	}
	

	
	
}
