package com.aco4;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Main {
	protected static int[] query = { 1, 6, 2, 8, 4, 11, 26, 38, 44 };
	protected static int Kday = 3;
	protected static int POInum = 50;
	protected static int iternum;
	protected static Double[][] pheromone;

	public static void main(String[] args) {
		System.out.println("Main");
		//for( int r = 2; r <= 10; r++ ){
		/*Previous processing*/
		JobClient preclient = new JobClient();
		JobConf preconf = new JobConf(com.aco4.Main.class);
		// TODO: specify output types
		preconf.setOutputKeyClass(Text.class);
		preconf.setOutputValueClass(Text.class);
		// TODO: specify input and output DIRECTORIES (not files)
		preconf.setInputFormat(TextInputFormat.class);
		preconf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(preconf, new Path("10round-9/part-00000"));
		FileOutputFormat.setOutputPath(preconf, new Path("intputData"));
		// TODO: specify a mapper and reducer
		preconf.setMapperClass(preMap.class);
		preconf.setReducerClass(preReduce.class);
		preclient.setConf(preconf);
		try{
			if(FileSystem.get(preconf).exists(new Path("intputData")))
				FileSystem.get(preconf).delete(new Path("intputData"));
			JobClient.runJob(preconf);
		}catch(Exception e){
			e.printStackTrace();
		}
		iternum = preReduce.count;
		pheromone = new Double[iternum][iternum];
		/*Plan Stage*/
		for( int k = 0; k < Kday; k++ ){
			JobClient client = new JobClient();
			JobConf conf = new JobConf(com.aco4.Main.class);
			// TODO: specify output types
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			// TODO: specify input and output DIRECTORIES (not files)
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			FileInputFormat.setInputPaths(conf, new Path("intputData"));
			FileOutputFormat.setOutputPath(conf, new Path("ACO_output"));
			// TODO: specify a mapper and reducer
			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);
			client.setConf(conf);
			try {
				//upload pheromone
				FileSystem out = FileSystem.get(conf);
				for( int i = 0; i < pheromone.length; i++ ){
					for( int j = 0; j < pheromone[i].length; j++ ){
						pheromone[i][j] = 1.0;
					}
				}
				FileSystem.get(conf).delete(new Path("pheromone"));
				BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out.create(new Path("pheromone"))));
				for( int i = 0; i < pheromone.length; i++ ){
					for( int j = 0; j < pheromone[i].length; j++ ){
						writer.write(pheromone[i][j]+" ");
					}writer.write("\n");
				}
				writer.close();
				out.close();
				//load pheromone
				FileSystem infs = FileSystem.get(conf);
				BufferedReader bReader = new BufferedReader(new InputStreamReader(infs.open(new Path("pheromone"))));
				String line = bReader.readLine();
				int count = 0;
				while(line!=null){
					String[] str_p = line.split(" ");
					for( int i = 0; i < str_p.length; i++ ){
						pheromone[count][i] = Double.parseDouble(str_p[i]);
					}
					count++;
					line = bReader.readLine();
				}
				bReader.close();
				infs.close();
				//ACO_output is exist or not
				if(FileSystem.get(conf).exists(new Path("ACO_output")))
					FileSystem.get(conf).delete(new Path("ACO_output"));
				//run job
				JobClient.runJob(conf);
				System.out.print("Done");
				//update pheromone
				FileSystem outfs = FileSystem.get(conf);
				Main.pheromone = Reduce.pheromone.clone();
				
				if( !outfs.exists(new Path("pheromone")) ){
					System.out.println("pheromone file not exsits!");
				}
				else{
					FileSystem.get(conf).delete(new Path("pheromone"));
					BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(outfs.create(new Path("pheromone"))));
					for( int i = 0; i < pheromone.length; i++ ){
						for( int j = 0; j < pheromone[i].length; j++ ){
							bWriter.write(pheromone[i][j]+" ");
						}bWriter.write("\n");
					}
					bWriter.close();
					outfs.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		//}/*/end for loop of round time*/
	}/*end main fun*/
}/*endMain class*/