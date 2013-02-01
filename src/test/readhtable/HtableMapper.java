package test.readhtable;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

public class HtableMapper extends TableMapper<Text, Text> {

	public void map(ImmutableBytesWritable key, Result result, Context context) {
		
		for (KeyValue kv : result.raw()) {
			try {
				context.write(new Text(kv.getRow()), new Text(kv.getFamily()));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
