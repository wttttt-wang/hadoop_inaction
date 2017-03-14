import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created with Eclipse.
 * Attention: for customized-Partitioner, u must jar the file.
 * 			  other than just run on Eclipse
 * Description:
 * Author: wttttt
 * Github: https://github.com/wttttt-wang/hadoop_inaction
 * Date: 2017-03-14
 * Hadoop Version: Hadoop 2.6.2
 */

public class PartitionerDemo {
	// DIY Partitioner
	public static class KpiPartitioner extends Partitioner<Text, KpiWritable>{
		@Override
		public int getPartition(Text key, KpiWritable value, int numPartitioner){
			// 实现不同长度号码分配到不同reduce task中
			/* 后续实现时，要配置job：
				设置Partitioner
	        		job.setPartitionerClass(KpiPartitioner.class);
	        		job.setNumReduceTasks(2);
	        */
			int numLength = key.toString().length();
			if (numLength == 11){
				return 0;
			}else{
				return 1;
			}
		}

	}
	
	static class xxMapper extends Mapper<LongWritable, Text, Text, KpiWritable>{
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			final String[] splited = value.toString().split("\t");
			// the second field is the msisdn(phone-number)
			final Text k2 = new Text(splited[1]);
			final KpiWritable v2 = new KpiWritable(splited[6],splited[7],splited[8],splited[9]);
			context.write(k2, v2);
		}
	}
	
	static class xxReducer extends Reducer<Text, KpiWritable, Text, KpiWritable>{
		protected void reduce(Text k2, Iterable<KpiWritable> v2s, Context context) 
				throws IOException, InterruptedException{
			long upPackNum = 0L;
			long downPackNum = 0L;
			long upPayLoad = 0L;
			long downPayLoad = 0L;
			for(KpiWritable v2 : v2s){
				upPackNum += v2.upPackNum;
				downPackNum += v2.downPackNum;
				upPayLoad += v2.upPayLoad;
				downPayLoad += v2.downPayLoad;
			}
			// do not forget to change upPackNum to String, by +""
			context.write(k2, new KpiWritable(upPackNum+"", 
					downPackNum+"", upPayLoad+"", downPayLoad+""));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: DIYWritable <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "DIY Writable");
	    job.setJarByClass(PartitionerDemo.class);
	    job.setMapperClass(xxMapper.class);
	    job.setCombinerClass(xxReducer.class);
	    job.setReducerClass(xxReducer.class);
	    job.setPartitionerClass(KpiPartitioner.class);
		job.setNumReduceTasks(2);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(KpiWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	//DIY a writable class implements the Writable interface
	static class KpiWritable implements Writable{
	 long upPackNum;
	 long downPackNum;
	 long upPayLoad;
	 long downPayLoad;
	 
	 public KpiWritable(){}
	 
	 public KpiWritable(String upPackNum, String downPackNum, String upPayLoad, String downPayLoad){
	     this.upPackNum = Long.parseLong(upPackNum);
	     this.downPackNum = Long.parseLong(downPackNum);
	     this.upPayLoad = Long.parseLong(upPayLoad);
	     this.downPayLoad = Long.parseLong(downPayLoad);
	 }
	 
	 
	 @Override
	 public void readFields(DataInput in) throws IOException {
	 		// DataInput: http://docs.oracle.com/javase/7/docs/api/java/io/DataInput.html
	 		// readLong(): Reads eight input bytes and returns a long value.
	     this.upPackNum = in.readLong();
	     this.downPackNum = in.readLong();
	     this.upPayLoad = in.readLong();
	     this.downPayLoad = in.readLong();
	 }

	 @Override
	 public void write(DataOutput out) throws IOException {
	     out.writeLong(upPackNum);
	     out.writeLong(downPackNum);
	     out.writeLong(upPayLoad);
	     out.writeLong(downPayLoad);
	 }
	 
	 @Override
	 public String toString() {
	     return upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t" + downPayLoad;
	 }
	}
}

