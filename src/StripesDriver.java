

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

public class StripesDriver {

	public static void main(String[] args) {
		
		JobConf conf = new JobConf(CorrelationDriver.class);
		 conf.setJobName("census");

		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(MapWritable.class);
		
		conf.setMapperClass(StripesMapper.class);
		conf.setCombinerClass(StripesReducer.class);
		// TODO: specify a reducer
		conf.setReducerClass(StripesReducer.class);

		// TODO: specify input and output DIRECTORIES (not files)
		conf.setInputFormat(TextInputFormat.class);
		 conf.setOutputFormat(SequenceFileOutputFormat.class);
		 
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
