package mapreduceAIPTSpart1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	/*public void reduce(WritableComparable _key, Iterator values,
			OutputCollector output, Reporter reporter) throws IOException {
		// replace KeyType with the real type of your key
		KeyType key = (KeyType) _key;

		while (values.hasNext()) {
			// replace ValueType with the real type of your value
			ValueType value = (ValueType) values.next();

			// process value
		}
	}*/
	private static Text value = new Text();
	private static Text outkey = new Text();
	private static Text outvalue = new Text();
	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		//System.out.println("HelloReducer");
		value = new Text(values.next());
		String value_str1 = value.toString();
		String[] str1 = value_str1.split("\t");
		int bestCost = Integer.parseInt(str1[1]);
		String bestPath = null, best = null;
		//for Path P:values do
		while (values.hasNext()) {
			value = new Text(values.next());
			String value_str2 = value.toString();
			String[] str2 = value_str2.split("\t");
			int Pcost = Integer.parseInt(str2[1]);
			//if P.cost < bestCost then
			if( Pcost < bestCost ){
				bestPath = str2[0];
				best = str2[1]+"\t"+str2[2];
				//bestPath = P
				str1[0] = str2[0];
				//bestCost = P.cost
				str1[1] = str2[1];
				str1[2] = str2[2];
			}
			else{
				bestPath = str1[0];
				best = str1[1]+"\t"+str1[2];
			}
			bestCost = Integer.parseInt(str1[1]);
		}
		try{
			if(bestPath != null){
				outkey.set(bestPath);
				outvalue.set(best);
				output.collect(outkey, outvalue);
				//System.out.println(outkey+"\t"+outvalue);
			}
			//else
				//throw new Exception("null");
		}catch( Exception e ){
			e.printStackTrace();
		}
	}
}