package model;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class HDFSImg {

	private Text filename;
	private BytesWritable bw;
	
	public HDFSImg(Text filename, byte[] bwArr){
		this.filename = new Text(filename);
		bw = new BytesWritable(bwArr);
	}

	public Text getFilename() {
		return filename;
	}

	public BytesWritable getBw() {
		return bw;
	}
	
	
}
