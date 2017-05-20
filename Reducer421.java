import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer421 extends Reducer<Text,IntWritable,Text,IntWritable>
{

	@Override
	protected void reduce(Text value,Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		int count=0;
		String key = value.toString() ; 
		
		key=key+" "+"is questioned(times) ";
		
		for(IntWritable value1 : values)
		{
			count = count + value1.get(); 
		}
		
		context.write(new Text(key),new IntWritable(count));
		
		
	}

	
	

}
