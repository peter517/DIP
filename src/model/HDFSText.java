package model;

import org.apache.hadoop.io.Text;

public class HDFSText {

	private Text filename;
	//file text content
	private Text content;

	public HDFSText(Text filename, Text content) {
		this.filename = new Text(filename);
		this.content = new Text(content);
	}

	public Text getFilename() {
		return filename;
	}

	public Text getContent() {
		return content;
	}
}
