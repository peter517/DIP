package test.seqtext;


import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

public class SeqTextReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context) {
		String k = key.toString();
		for (Text val : values) {
			System.out.println("key:" + k + "," + "value:" + val.toString());
		}
		
//		Put putrow = new Put(k.getBytes());
		// putrow.add("n".getBytes(), "qualifier".getBytes(), v.getBytes());
		// try {
		//
		// context.write(new ImmutableBytesWritable(key.getBytes()), putrow);
		//
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
	}

}
