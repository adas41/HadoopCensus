

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CorrelationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter){
		String line = value.toString();
		if(!line.trim().equals("")){
			String[] record = line.split(",");
			int age = Integer.parseInt(record[0].trim());
			String occ = record[6].trim();
			
			try {
				output.collect(new Text(age+", "+occ), one);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	

}
