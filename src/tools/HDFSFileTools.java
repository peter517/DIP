package tools;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import model.GlobalVars;
import model.HDFSImg;
import model.HDFSText;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


public class HDFSFileTools {

	/**
	 * readFile
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static HDFSText readTxtFile(String path) throws IOException {

		StringBuffer sb = new StringBuffer();
		FSDataInputStream fsinput = GlobalVars.getFs().open(new Path(path));

		byte[] buffer = new byte[1024];
		int byteNum = 0;
		while ((byteNum = fsinput.read(buffer)) != -1) {
			//read all data to a StringBuffer
			sb.append(new String(buffer));
		}
		return new HDFSText(new Text(path), new Text(sb.toString()));
	}
	
	/**
	 * readFile
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static HDFSImg readImgFile(String path) throws IOException {

		FSDataInputStream fsinput = GlobalVars.getFs().open(new Path(path));

		byte[] buffer = new byte[1024];
		int byteNum = 0;
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		while ((byteNum = fsinput.read(buffer)) != -1) {
			output.write(buffer);
		}
		fsinput.close();
		return new HDFSImg(new Text(path), output.toByteArray());
	}

	/**
	 * read a dir
	 * 
	 * @param dirPath
	 * @return
	 * @throws IOException
	 */
	public static List<HDFSImg> readImgDir(String dirPath) throws IOException {

		List<HDFSImg> hfList = new ArrayList<HDFSImg>();
		FileStatus[] fileStatusArray = GlobalVars.getFs().listStatus(new Path(dirPath));

		for (FileStatus fileStatus : fileStatusArray) {
			// if it is a file
			if (GlobalVars.getFs().isFile(fileStatus.getPath())) {
				hfList.add(readImgFile(fileStatus.getPath().toString()));
				
				
			} 
		}
		return hfList;
	}
	
	/**
	 * read a dir
	 * 
	 * @param dirPath
	 * @return
	 * @throws IOException
	 */
	public static List<HDFSText> readTextDir(String dirPath) throws IOException {

		List<HDFSText> hfList = new ArrayList<HDFSText>();
		FileStatus[] fileStatusArray = GlobalVars.getFs().listStatus(new Path(dirPath));

		for (FileStatus fileStatus : fileStatusArray) {
			// if it is a file
			if (GlobalVars.getFs().isFile(fileStatus.getPath())) {
				hfList.add(readTxtFile(fileStatus.getPath().toString()));
			} 
		}
		return hfList;
	}
	
	/**
	 * delelt all files in dir
	 * 
	 * @param dirPath
	 * @return
	 * @throws IOException
	 */
	public static void deletAllFiles(String dirPath) throws IOException {

		FileStatus[] fileStatusArray = GlobalVars.getFs().listStatus(new Path(dirPath));

		for (FileStatus fileStatus : fileStatusArray) {
			// if it is a file
			if (GlobalVars.getFs().isFile(fileStatus.getPath())) {
				deleteFile(fileStatus.getPath().toString());
			} 
		}
	}

	/**
	 * writeFile
	 * 
	 * @param path
	 * @param content
	 * @throws IOException
	 */
	public static void writeFile(String path, byte[] content) throws IOException {
		FSDataOutputStream outputStream = GlobalVars.getFs().create(new Path(path));
		outputStream.write(content, 0, content.length);
	}

	/**
	 * deleteFile
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static boolean deleteFile(String path) throws IOException {
		// no recursive delete
		return GlobalVars.getFs().delete(new Path(path), false);
	}
	
	/**
	 * fromLocalToHDFS
	 * @param localPath
	 * @param hdfsPath
	 * @throws Exception
	 */
	public static void fromLocalToHDFS(String localPath, String hdfsPath) throws Exception 
	{
		Path srcPath = new Path(localPath);
		Path dstPath = new Path(hdfsPath);
		GlobalVars.getFs().copyFromLocalFile(srcPath, dstPath);

	}
	
	/**
	 * createHdfsFile
	 * @param FileName
	 * @throws Exception
	 */
	public static void createHdfsFile(String FileName) throws Exception
	{
		 GlobalVars.getFs().create(new Path(FileName));
		
	}

	/**
	 * isFileExist
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static boolean isFileExist(String path) throws IOException {
		return GlobalVars.getFs().exists(new Path(path));
	}

	/**
	 * writeText2SequenceFile
	 * @param filePath
	 * @param fileList
	 */
	public static void writeImg2SequenceFile(String filePath, List<HDFSImg> fileList) {

		SequenceFile.Writer writer = null;
		try {
			//Text.class, Text.class means the key and value
			writer = SequenceFile.createWriter(GlobalVars.getFs(), GlobalVars.getConf(), new Path(filePath), Text.class, BytesWritable.class);
			for (HDFSImg file : fileList) {
//				System.out.print(file.getFilename().toString());
				writer.append(file.getFilename(), file.getBw());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}
	
	/**
	 * write2SequenceFile
	 * @param filePath
	 * @param fileList
	 */
	public static void writeText2SequenceFile(String filePath, List<HDFSText> fileList) {

		SequenceFile.Writer writer = null;
		try {
			//Text.class, Text.class means the key and value
			writer = SequenceFile.createWriter(GlobalVars.getFs(), GlobalVars.getConf(), new Path(filePath), Text.class, Text.class);
			for (HDFSText file : fileList) {
				writer.append(file.getFilename(), file.getContent());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	/**
	 * readSequenceFile
	 * 
	 * @param sequeceFilePath
	 * @return
	 */
	public static List<HDFSImg> readImgSequenceFile(String sequeceFilePath) {

		List<HDFSImg> result = new ArrayList<HDFSImg>();
		SequenceFile.Reader reader = null;
		Text filename = new Text();
		BytesWritable content = new BytesWritable();

		try { 
			reader = new SequenceFile.Reader(GlobalVars.getFs(), new Path(sequeceFilePath), GlobalVars.getConf());
			while (reader.next(filename, content)) {
				result.add(new HDFSImg(filename, content.getBytes()));
				//read new contents
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
		return result;
	}
	
	/**
	 * readSequenceFile
	 * 
	 * @param sequeceFilePath
	 * @return
	 */
	public static List<HDFSText> readTextSequenceFile(String sequeceFilePath) {

		List<HDFSText> result = new ArrayList<HDFSText>();
		SequenceFile.Reader reader = null;
		Text filename = new Text();
		Text content = new Text();

		try { 
			reader = new SequenceFile.Reader(GlobalVars.getFs(), new Path(sequeceFilePath), GlobalVars.getConf());
			while (reader.next(filename, content)) {
				result.add(new HDFSText(filename, content));
				//read new contents
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
		return result;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

//		String dirPath = "hdfs://localhost:8021/user/root/intput01";
//		// System.out.println(HDFSFileTools.isFileExist(filePath));
//
////		HDFSFileTools.readFile("hdfs://localhost:8021/user/root/intput01/file01");
//		List<HDFSText> hfList = HDFSFileTools.readTextDir(dirPath);
//
//		String dstFilePath = "hdfs://localhost:8021/user/root/input01/sq01";
////		//
//		writeText2SequenceFile(dstFilePath, hfList);
//		List<HDFSText> readList = readText2SequenceFile(dstFilePath);
//		//
//		// 对比数据是否正确并输出
//		for (HDFSText hdFile : readList) {
//			System.out.println(hdFile.getFilename() + "\t" + hdFile.getContent());
//		}
		
		String dirPath = "hdfs://localhost:8021/user/root/imgInputDir";
		List<HDFSImg> hfList = HDFSFileTools.readImgDir(dirPath);

		String dstFilePath = "hdfs://localhost:8021/user/root/imgOutputDir/imgSeq01";
//		//
		writeImg2SequenceFile(dstFilePath, hfList);
//		List<HDFSImg> readList = readImg2SequenceFile(dstFilePath);
//		// 对比数据是否正确并输出
//		for (HDFSImg hdFile : readList) {
//			InputStream in = new ByteArrayInputStream(hdFile.getBw().getBytes());
//			BufferedImage bImageFromConvert = ImageIO.read(in);
//			ImageIO.write(bImageFromConvert, "jpg", new File("/root/eclipse/img.jpg"));
//		}

	}

}
