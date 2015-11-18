package cloud.project1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

public class TextArrayWritable implements Writable {
	
	private Text value;

	public TextArrayWritable() {
		this.value = new Text();
	}
	
	public void set(Text value){
		System.out.println("IN Set ");
		this.value=value;
	}
	
	/*@Override
	public Writable[] get() {
		System.out.println("INside Get "+value[0]);
		return super.get();		
	}*/
	
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		value.readFields(in);	
	}
	
	
	public Text getValue() {
		return value;
	}


	public void setValue(Text value) {
		this.value = value;
	}


	@Override
	public void write(DataOutput out) throws IOException
	{
		value.write(out);	
	}

}
