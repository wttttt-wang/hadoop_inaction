import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * Created with Eclipse.
 * Description:
 * Author: wttttt
 * Github: https://github.com/wttttt-wang/hadoop_inaction
 * Date: 2017-03-13
 * Hadoop Version: Hadoop 2.6.2
 */
public class CounterDemo {
	public static class xxMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		protected void map(LongWritable key, Text value, 
				Mapper<LongWritable, Text, Text, LongWritable>.Context context) 
				throws IOException, InterruptedException{
			// get Counter through class Mapper.Context
			// getCounters(nameOfCounterGroup, nameOfCounterName)
			// Counter: http://hadoop.apache.org/docs/r2.6.2/api/org/apache/hadoop/mapreduce/Counter.html
			Counter sensitiveCounter = context.getCounter("Sensitive Words:", "never");
			String line = value.toString();
			// assume that "never" is a sensitive word
			if(line.contains("never")){
				// void increment(long incr): Increment this counter by the given value
				sensitiveCounter.increment(1L);
			}
			String[] splited = line.split(" ");
			for(String word : splited){
				context.write(new Text(word), new LongWritable(1L));
			}
			
		}
	}
	
	public static class xxReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		private LongWritable result = new LongWritable();
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) 
				throws IOException, InterruptedException{
			long sum = 0;
			for(LongWritable value : values){
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    if (otherArgs.length != 2) {
		      System.err.println("Usage: wordcount <in> <out>");
		      System.exit(2);
		    }
		    Job job = new Job(conf, "word count");
		    job.setJarByClass(CounterDemo.class);
		    job.setMapperClass(xxMapper.class);
		    job.setCombinerClass(xxReducer.class);
		    job.setReducerClass(xxReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }

}
