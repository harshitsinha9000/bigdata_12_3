import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver42 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
        String Output_dir2 = "/sample_output2";
		
		boolean success;
		Configuration conf = new Configuration();
		Job job = new Job(conf,"DemoTask1");
		job.setJarByClass(Driver42.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Mapper42.class);
		job.setReducerClass(Reducer42.class);
		
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		/*
		Path out=new Path(args[1]);
		out.getFileSystem(conf).delete(out);
		*/
		
		success=job.waitForCompletion(true);
		
		if(success)
		{
			Configuration conf2 = new Configuration();
			Job job2 = new Job(conf2,"DemoTask2");
			job2.setJarByClass(Driver42.class);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);
			
			job2.setMapperClass(Mapper421.class);
			job2.setReducerClass(Reducer421.class);
			
			 
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job2, new Path(args[0])); 
			FileOutputFormat.setOutputPath(job2,new Path(args[2]));
			
			job2.waitForCompletion(true);
		}


	}
}	
