

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class CorrelationDriver {

	public static void main(String[] args) {
		
		JobConf conf = new JobConf(CorrelationDriver.class);
		 conf.setJobName("census");

		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(CorrelationMapper.class);
		conf.setCombinerClass(CorrelationReducer.class);
		// TODO: specify a reducer
		conf.setReducerClass(CorrelationReducer.class);

		// TODO: specify input and output DIRECTORIES (not files)
		conf.setInputFormat(TextInputFormat.class);
		 conf.setOutputFormat(TextOutputFormat.class);
		 
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		 FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		// TODO: specify a mapper
		 try {

		 JobClient.runJob(conf);
		
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
