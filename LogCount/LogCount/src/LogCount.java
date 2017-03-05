import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// LogCount: count IP Address's visits
// map(): map each line to <IPAddress, 1>
// reduce(): add to <IPAddress, n>
// use combiner() to optimize

public class LogCount{
	public static class xxMapper 
		extends Mapper<Object, Text, Text, IntWritable>{   // extends继承类
		private final static IntWritable one = new IntWritable(1); // final常量
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException{
			// for each line
			word.set(value.toString().split(" ")[0]);
			context.write(word,one);
			}
		}
	
	public static class xxReducer
		extends Reducer<Text, IntWritable, Text, IntWritable>{   // extends继承类
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException{
			int sum = 0;
			// for each key, count it
			for (IntWritable val : values){
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}	
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		// 从输入获取剩下的配置：包括输入和输出路径
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// 输入不合理检测
		if (otherArgs.length < 2){
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Log Count");
		job.setJarByClass(LogCount.class);
		job.setMapperClass(xxMapper.class);
		// combiner和reducer使用同一个class，当如果combiner处理逻辑相同时
		// 否则，为combiner写一个类，一般xxcombiner也是继承自Reducer
		job.setCombinerClass(xxReducer.class);  // combiner and reducer use the same class
		job.setReducerClass(xxReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i){
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
