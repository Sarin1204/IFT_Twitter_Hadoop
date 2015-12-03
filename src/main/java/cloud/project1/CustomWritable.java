package cloud.project1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

public class CustomWritable implements Writable {
	
	private IntWritable follower;
	private Text place;
	private IntWritable retweet;

	public IntWritable getRetweet() {
		return retweet;
	}


	public void setRetweet(IntWritable retweet) {
		this.retweet = retweet;
	}


	public Text getPlace() {
		return place;
	}


	public void setPlace(Text place) {
		this.place = place;
	}


	public CustomWritable() {
		this.follower = new IntWritable();
		this.place=new Text();
		this.retweet=new IntWritable();
	}
	
	
	public void set(IntWritable follower,Text place,IntWritable retweet){
		/*System.out.println("IN Set ");*/
		this.follower=follower;
		this.place=place;
		this.retweet=retweet;
	}
	
	/*@Override
	public Writable[] get() {
		System.out.println("INside Get "+value[0]);
		return super.get();		
	}*/
	
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		follower.readFields(in);
		place.readFields(in);
		retweet.readFields(in);
	}
	
	
	public IntWritable getValue() {
		return follower;
	}


	public void setValue(IntWritable value) {
		this.follower = value;
	}


	public void write(DataOutput out) throws IOException
	{
		follower.write(out);	
		place.write(out);
		retweet.write(out);
	}


}
