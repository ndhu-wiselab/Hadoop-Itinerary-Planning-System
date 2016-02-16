package com.aco4;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	private static Double MAXp;
	private static String MAXid;
	private static String MAXiter;
	private static int MAXiter_weight;
	private static int MAXiter_pathlength;
	private static int sumWeight;
	private static int sumPathlength;
	protected static String iter_buffer = "";
	protected static String id_buffer = "";
	protected static Double[][] pheromone;
	
	Reduce(){
		MAXp = 0.0;
		MAXid = "";
		MAXiter = "";
		MAXiter_weight = 0;
		MAXiter_pathlength = 0;
	}
	
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		System.out.println("Reduce");
		while (values.hasNext()) {
			
			}
		}
		//filter the duplicated POIs
		boolean[] mark = new boolean[Main.POInum];
		for( int i = 0; i < mark.length; i++ )
			mark[i] = false;
		
		String[] POIs = MAXiter.split(":");
		String temp = "";
		for(int i = 0; i < POIs.length; i++){
			if(mark[Integer.parseInt(POIs[i])] == true){
				System.out.println("delete POI: "+POIs[i]);
			}
			else{
				if( i == 0 )
					temp = POIs[i];
				else
					temp = temp+":"+POIs[i];
			}
		}
		MAXiter = temp;
		//record recently selected iter into buffer
		if( iter_buffer == "" ){
			iter_buffer = MAXiter;
			id_buffer = MAXid;
		}
		else{
			iter_buffer = iter_buffer+"\n"+MAXiter;
			id_buffer = id_buffer+"\t"+MAXid;
		}
		
			
	}/*end reduce fun*/
}/*end Reduce class*/
