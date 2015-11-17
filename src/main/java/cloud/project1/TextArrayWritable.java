package cloud.project1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

public class TextArrayWritable extends ArrayWritable {
	
	private Text[] value;

	public TextArrayWritable() {
		super(TextArrayWritable.class);
	}
	
	
	public void set(Text[] value){
		System.err.println("IN Set "+value[0]);
		this.value=value;
	}
	
	@Override
	public Writable[] get() {
		return super.get();		
	}
	
	@Override
	public void readFields(DataInput i) throws IOException
	{
		
	}
	
	
	@Override
	public void write(DataOutput d) throws IOException
	{
		for(Text t:value)
		{
			System.err.println("Write" +t);
			t.write(d);
		}
		
	}

}
