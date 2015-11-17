package cloud.project1;

import java.io.IOException;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.ArrayWritable;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cloud.project1.TextArrayWritable;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;


public class Mapper1 extends Mapper<LongWritable,Text,Text,TextArrayWritable>{
	Logger log= LoggerFactory.getLogger(Mapper1.class);
	TextArrayWritable tw =new TextArrayWritable();
//	HashMap<String,Status> output=new HashMap<String,Status>();
	public String[] hc1={"Hillary","Clinton"};
	public String[] bc1={"Ben","Carson"};
	public String[] dj1={"Donald J.","Donald J. Trump","Donald","Trump"};
	public String[] jb1={"Jeb","Bush"};
	public String[] bs1={"Bernie","Sanders"};
	public String[] mm1={"Martin O'Malley","Martin","Malley"};
	public String[] tc1={"Ted","Cruz"};
	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		Status s;
		try {
			s = TwitterObjectFactory.createStatus(value.toString());
				String status=s.getText();
		for(String hcs:hc1)
		{
			if(status.contains(hcs))
			{
				log.info(" "+hcs);
				Text[] t=new Text[1];
				t[0]=new Text(s.getText());
				log.info("Text "+t);
				tw.set(t);
				log.info("Mapper Output "+tw.toString());
				context.write(new Text("Hillary Clinton"),tw);
				break;
			}
		}
/*		for(String djs:dj1)
		{
			if(status.contains(djs))
			{
				log.info(" "+djs);
				String[] s2={s.getPlace().getFullName(),s.getCreatedAt().toString(),Integer.toString(s.getRetweetCount()),Integer.toString(s.getFavoriteCount())};
				context.write(new Text("Donald Trump"),new TextArrayWritable(s2));
				break;
			}
		}
		for(String jbs:jb1)
		{
			if(status.contains(jbs))
			{	
				log.info(" "+jbs);
				String[] s3={s.getPlace().getFullName(),s.getCreatedAt().toString(),Integer.toString(s.getRetweetCount()),Integer.toString(s.getFavoriteCount())};
				context.write(new Text("Jeb Bush"),new TextArrayWritable(s3));
				break;
			}
		}
		for(String bcs:bs1)
		{
			if(status.contains(bcs))
			{
				log.info(" "+bcs);
				String[] s4={s.getPlace().getFullName(),s.getCreatedAt().toString(),Integer.toString(s.getRetweetCount()),Integer.toString(s.getFavoriteCount())};
				context.write(new Text("Bernie Sanders"),new TextArrayWritable(s4));
				break;
			}
		}
		for(String mms:mm1)
		{
			if(status.contains(mms))
			{
				String[] s5={s.getPlace().getFullName(),s.getCreatedAt().toString(),Integer.toString(s.getRetweetCount()),Integer.toString(s.getFavoriteCount())};
				context.write(new Text("Martin O'Malley"),new TextArrayWritable(s5));
				break;
			}
		}
		for(String tcs:tc1)
		{
			if(status.contains(tcs))
			{
				String[] s1={s.getPlace().getFullName(),s.getCreatedAt().toString(),Integer.toString(s.getRetweetCount()),Integer.toString(s.getFavoriteCount())};
				context.write(new Text("Ted Cruz"),new TextArrayWritable(s1));
				break;
			}
		}	
	} catch (Exception e) {
		System.err.println(e.getStackTrace());
		System.err.println(e);
		System.err.println(e.getMessage());
		System.err.println(e.toString());
	}
	
*/		
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	
}
