import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer42 extends Reducer<Text,IntWritable,Text,IntWritable>
{

	@Override
	protected void reduce(Text value,Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		int count=0,percentage_ans=0;
		String key = "Answer Percantage for Hospital "; 
		key=key+" "+value.toString()+"= ";
		
		int no_of_times_answered=0;
		for(IntWritable value1 : values)
		{
			count = count + value1.get();
			no_of_times_answered++;
		}
		
		percentage_ans = (count/no_of_times_answered); 
		
		context.write(new Text(key),new IntWritable(percentage_ans));
		
		
	}

	
	

}
