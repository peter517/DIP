package test.seqImg;

import java.io.File;

import model.GlobalVars;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;


public class SeqImgDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {

		Configuration conf = GlobalVars.getHbConf();

		Job job = new Job(conf, "ReadSeqFileToHbase");
		job.setJarByClass(Main.class);

		Path in = new Path(GlobalVars.ROOT_DIR + File.separator + GlobalVars.SEQ_IMG_DIR + File.separator + "imgSeq01");
		FileInputFormat.addInputPath(job, in);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(SeqImgMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		TableMapReduceUtil.initTableReducerJob("s4", SeqImgReducer.class, job);

		job.waitForCompletion(true);
		return 0;
	}

}
