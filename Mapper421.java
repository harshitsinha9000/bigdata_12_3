import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Mapper421 extends Mapper<LongWritable, Text,Text,IntWritable> {
	public void map(LongWritable key, Text value,Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("/t");
		
		context.write(new Text(lineArray[1]),new IntWritable(1));
		
	}
}