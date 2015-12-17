package average;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MovieMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, Text> {

	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		Text outvalue = new Text();
		String[] pieces = value.toString().split("\\|");
		outvalue.set("A" + value.toString());
	
		output.collect(new IntWritable(new Integer(pieces[0]).intValue()),outvalue);
	}
}