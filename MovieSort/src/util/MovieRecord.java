package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MovieRecord implements WritableComparable<MovieRecord> {

	private IntWritable movieId;
	private Text movieName;
	private IntWritable movieYear;

	public MovieRecord() {
		set(new IntWritable(), new Text(), new IntWritable());
	}

	private void set(IntWritable movieId, Text movieName, 
			IntWritable movieYear) {

		this.movieId = movieId;
		this.movieName = movieName;
		this.movieYear = movieYear;

	}

	@Override
	public void readFields(DataInput in) throws IOException {

		movieId.readFields(in);
		movieName.readFields(in);
		movieYear.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {

		movieId.write(out);
		movieName.write(out);
		movieYear.write(out);
	}

	@Override
	public int hashCode() {

		return movieId.hashCode() + movieName.hashCode() + movieYear.hashCode();

	}

	@Override
	public boolean equals(Object o) {

		if (o instanceof MovieRecord) {
			MovieRecord sr = (MovieRecord) o;
			return movieId.equals(sr.movieId) && movieName.equals(sr.movieName)
					&& movieYear.equals(sr.movieYear);
		}
		return false;

	}

	@Override
	public String toString() {

		return movieId + "," + movieName + "," + movieYear;

	}

	@Override
	public int compareTo(MovieRecord sr) {

		int cmp = movieId.compareTo(sr.movieId);
		if (cmp != 0) {
			return cmp;
		}

		cmp = movieName.compareTo(sr.movieName);
		if (cmp != 0) {
			return cmp;
		}

		return movieYear.compareTo(sr.movieYear);
	}

	public IntWritable getMovieId() {
		return movieId;
	}

	public void setMovieId(IntWritable movieId) {
		this.movieId = movieId;
	}

	public Text getMovieName() {
		return movieName;
	}

	public void setMovieName(Text movieName) {
		this.movieName = movieName;
	}

	public IntWritable getMovieYear() {
		return movieYear;
	}

	public void setMovieYear(IntWritable movieYear) {
		this.movieYear = movieYear;
	}

}