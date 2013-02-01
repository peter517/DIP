package test.seqtext;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SeqTextMapper extends Mapper<Text, Text, Text, Text> {

	public void map(Text filename, Text content, Context context) {
		
		System.out.println(filename.toString() + "\t" + content.toString());
	}

}
