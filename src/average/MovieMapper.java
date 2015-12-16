package average;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.MovieRecord;

public class MovieMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, MovieRecord> {

	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, MovieRecord> output, Reporter reporter)
			throws IOException {

		String[] pieces = value.toString().split("\\|");
		
		MovieRecord record = new MovieRecord();
		
		record.setMovieId(new IntWritable(new Integer(pieces[0]).intValue()));
		record.setMovieName(new Text(pieces[1]));
		record.setMovieYear(new IntWritable(new Integer(pieces[2]).intValue()));
		
		if (record.getMovieYear().get()>0){
		output.collect(new IntWritable(new Integer(pieces[2]).intValue()), record);
		}
	}
}