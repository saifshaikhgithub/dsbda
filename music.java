import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class musictrack {
	public static void main(String[] args) throws Exception{
		Configuration c = new Configuration();
		Job j = new Job(c, "musicmap");
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		
		j.setJarByClass(musictrack.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		j.setMapperClass(MapForWord.class);
		j.setReducerClass(RedForWord.class);
		
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		
		System.exit(j.waitForCompletion(true)?0:1);
		
	}
	
	public static class MapForWord extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			String[] lines = value.toString().split("\n");
			
			for(String line : lines){
				String[] words = line.split(",");
				
				Text outkey = new Text(words[0].trim());
				IntWritable outvalue = new IntWritable(1);
				con.write(outkey, outvalue);
			}
		}
	}
	
	public static class RedForWord extends Reducer<Text, IntWritable, Text, IntWritable>{
		private static int cnt = 0;
		public void reduce(Text word,  Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
			
			HashSet<Text> uniq = new HashSet<>();
			for(IntWritable list : values){
				uniq.add(new Text(list.toString()));	
			}
			int ans = uniq.size();
			cnt += ans;
			
			Text message = new Text("The Number of the uniques Listeners are " + cnt);
			con.write(message, new IntWritable(cnt));
		}
		
	}
}
