package test.seqImg;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SeqImgMapper extends Mapper<Text, Text, Text, Text> {

	public void map(Text filename, BytesWritable content, Context context) {
		
		System.out.println(filename.toString() + "\t" + filename.toString() );
	}

}
