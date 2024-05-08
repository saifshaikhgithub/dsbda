import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class music {
	public static void main(String[] args) throws Exception{
		
		Configuration c = new Configuration();
		Job j = new Job(c, "musicfile");
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		j.setMapperClass(Mapforword.class);
		j.setReducerClass(Redforword.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		j.setJarByClass(music.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
		
	}
	
	public static class Mapforword extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			String[] lines = value.toString().split("\n");
			
			for(String words : lines){
				String[] word = words.split(",");
				
				if(word.length >= 2){
					Text outKey = new Text(word[1]);
					IntWritable outValue = new IntWritable(Integer.parseInt(word[2]));
					con.write(outKey, outValue);
				}
			}
		}
	}
	
	
	public static class Redforword extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
			int sum = 0;
			
			for(IntWritable value : values){
				sum += value.get();
			}
			
			con.write(key, new IntWritable(sum));
		}	
	}
}
