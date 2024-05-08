import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class logfile {
	public static void main(String [] args) throws Exception{
		Configuration c=new Configuration();	
		String[] files = new  GenericOptionsParser(c,args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path ouptut = new Path(files[1]);
		Job j = new Job(c, "logfile1");
		j.setJarByClass(logfile.class);
		j.setMapperClass(MapforWord.class);
		j.setReducerClass(ReduceforWord.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, ouptut);
		System.exit(j.waitForCompletion(true)?0:1);
	}
	
	public static class MapforWord extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			String[] lines = value.toString().split("\n");
			
			for(String line : lines){
				String[] singleword = line.split(",");
				Text outkey = new Text(singleword[1]);
				IntWritable outvalue = new IntWritable(1);
				con.write(outkey, outvalue);
			}

		}
	}
	
	public static class ReduceforWord extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
			int sum = 0;
			Text maxword = new Text();
			int maxi = 0;
			for(IntWritable value: values){
				sum += value.get();
			}
			if(sum > maxi){
				maxi = sum;
				maxword.set(word);
			con.write(maxword, new IntWritable(maxi));
			}
		}
	}
}
