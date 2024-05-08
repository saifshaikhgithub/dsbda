import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class tracks {
	public static void main(String[] args) throws Exception{
		
		Configuration c = new Configuration();
		Job j = new Job(c,"track");
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		
		j.setJarByClass(tracks.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		j.setMapperClass(Map.class);
		j.setReducerClass(Red.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		
		System.exit(j.waitForCompletion(true)?0:1);
		
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{	
		
		public void map(LongWritable key, Text value, Context con) throws InterruptedException, IOException{
			String[] lines = value.toString().split("\n");
			
			for(String line : lines){
				String[] words = line.split(",");
				
				Text outkey = new Text(words[1]);
				IntWritable outvalue = new IntWritable(Integer.parseInt(words[3]));
				con.write(outkey, outvalue);
			}
		}
	}
	
	public static class Red extends Reducer<Text ,IntWritable, Text, IntWritable>{
		public void red(Text key, Iterable<IntWritable> values, Context con)throws InterruptedException, IOException{
			int sum = 0;
			
			for(IntWritable value : values){
				sum += value.get();
			}
			
			con.write(key, new IntWritable(sum));
		}
	}
}
