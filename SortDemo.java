import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
 * Date: 2017-03-15
 * Hadoop Version: Hadoop 2.6.2
 */

public class SortDemo {
	
	static class xxMapper extends Mapper<LongWritable, Text, NewKey, LongWritable>{
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			final String[] splited = value.toString().split("\t");
			context.write(new NewKey(Long.parseLong(splited[0]), 
							Long.parseLong(splited[1])), 
							new LongWritable(Long.parseLong(splited[1])));
		}
	}
	
	static class xxReducer extends Reducer<NewKey, LongWritable, LongWritable, 
		LongWritable>{
		protected void reduce(NewKey k2, Iterable<LongWritable> v2s, 
				Context context) throws IOException, InterruptedException{
			context.write(new LongWritable(k2.first), new LongWritable(k2.second));
		}
	}

	static class NewKey implements WritableComparable<NewKey>{
		Long first;     // the first field
		Long second;    // the second field
		
		// constructor
		public NewKey(){}
		public NewKey(long first, long second){
			this.first = first;
			this.second = second;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeLong(first);
			out.writeLong(second);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			this.first = in.readLong();
			this.second = in.readLong();
		}

		// called when use this key to sort
		@Override
		public int compareTo(NewKey o) {
			// TODO Auto-generated method stub
			final long minus = this.first - o.first;
			if(minus != 0){
				return (int)minus;
			}
			// if the first field equals, then sort the second field
			return (int)(this.second - o.second);
		}
		
		// this is a function from java.lang.Object
		@Override
		public int hashCode(){
			return this.first.hashCode() + this.second.hashCode();
		}
		
		// this is also a function from java.lang.Object
		@Override
		public boolean equals(Object obj){
			if (!(obj instanceof NewKey)){
				return false;
			}
			//casting from 'Object' to 'NewKey'
			NewKey ok2 = (NewKey) obj;
			return (this.first == ok2.first) && (this.second == ok2.second);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: sortdemo <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "sort demo");
	    job.setJarByClass(SortDemo.class);
	    job.setMapperClass(xxMapper.class);
	    // 这里不能再设置combiner，否则输出格式不对，会报错
	    // job.setCombinerClass(xxReducer.class);
	    job.setMapOutputKeyClass(NewKey.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setReducerClass(xxReducer.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(LongWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}

 