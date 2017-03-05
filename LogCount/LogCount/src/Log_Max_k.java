import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
TopK 问题
Log_Max_k: find the max_k visiter's IP Address
map(): get TopK for each mapper 
	   * Use TreeMap to store topK for each mapper
	   * For each mapper: 
	   		for each record, we try to updata the treemap, and finally we get TopK
	   * TreeMap is somewhat like a 'large root heap'.
	   * Unlike usual(write after one line), we write after all the input split is handled.
	        this is realized by the function 'cleanup'(conducted after the mapper task).
reduce(): get the global TopK in one Reducer
		 * we need just one Reducer to ensure top-k
TopK的k值是从外部(命令行)传给Mapper&Reducer
      利用conf.set()以及conf.get()
**/

public class Log_Max_k {
	public static class xxMap extends 
						Mapper<LongWritable, Text, Text, IntWritable>{
		/**
		 * the map function
		 * input file: format as: IPAddress\tVisitNum  (for each line)
		 */
		// TODO: <String, Integer> or <Text, Integer>
		private TreeMap<Integer, Text> tree = new TreeMap<Integer, Text>();
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException{
			// TODO: conf.set() in function run()
			//在map方法中通过Context对象获取conf对象，进而取得参数值  
		    Configuration conf = context. getConfiguration();  
			int K = conf.getInt("K_value", 10); // default = 10
			String[] values = value.toString().split("\t");   // Tab split 
			//int visit_num = Integer.parseInt(values[1]);
			//String IPAddress = values[0];
			Text txt = new Text();
			txt.set(values[0]);
			tree.put(Integer.parseInt(values[1]), txt);
			if (tree.size() > K){
				tree.remove(tree.firstKey());  // store the top-k
			}	
		}	

		@Override
		protected void cleanup(Context context) throws IOException,
						InterruptedException{
			/**
		 * write after all the input split is handled, by the function cleanup()
			 */
			// iterate on the treemap, use Iterator
			Iterator iter = tree.entrySet().iterator();
			while (iter.hasNext()){
				@SuppressWarnings("unchecked")
				Map.Entry<Integer, Text> ent = (Map.Entry<Integer, Text>)iter.next();
				// Map.Entry ent = (Map.Entry)iter.next();
				// write: IPAddress Visit_num
				context.write(ent.getValue(), new IntWritable(ent.getKey().intValue()));
			}
		}
	}
	
	public static class xxReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private TreeMap<IntWritable, Text> tree = new TreeMap<IntWritable, Text>();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			int K = conf.getInt("K_value", 10);   // default = 10
			for(IntWritable visit_num: values){
				tree.put(visit_num, key);
				if (tree.size() > K){
					tree.remove(tree.firstKey());
				}
			}
			// iterate on tree, to write top-k
			Iterator iter = tree.entrySet().iterator();
			while (iter.hasNext()){
				Map.Entry<IntWritable, Text> ent =(Map.Entry<IntWritable, Text>)iter.next();
				context.write(ent.getValue(), ent.getKey());
			}	
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		// 从输入获取剩下的配置：包括输入和输出路径
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// 输入不合理检测
		if (otherArgs.length < 3){
			System.err.println("Usage: wordcount <K> <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "TopKIP");
		job.setJarByClass(Log_Max_k.class);
		job.setMapperClass(xxMap.class);
		// job.setCombinerClass(xxReducer.class);  // combiner and reducer use the same class
		job.setReducerClass(xxReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		conf.set("K_value", otherArgs[0]);
		for (int i = 1; i < otherArgs.length - 1; ++i){
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
