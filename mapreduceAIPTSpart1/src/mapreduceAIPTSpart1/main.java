package mapreduceAIPTSpart1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class main {
	public static void main(String[] args) {
		int counter = 1;
		Path input = new Path("INPUTGRAPH50");
		Path output = null;
		while( counter < 10 ){
			output = new Path("10round-"+counter);
			JobClient client = new JobClient();
			JobConf conf = new JobConf(main.class);
			// TODO: specify output types
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			// TODO: specify input and output DIRECTORIES (not files)
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			FileInputFormat.setInputPaths(conf, input);
			FileOutputFormat.setOutputPath(conf, output);
			
			// TODO: specify a mapper
			conf.setMapperClass(mapper.class);
					// TODO: specify a reducer
			conf.setReducerClass(reducer.class);
				client.setConf(conf);
			try {
				JobClient.runJob(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
			input = output;
			counter++;
			System.out.println("Finished "+counter);
		}
	}
}