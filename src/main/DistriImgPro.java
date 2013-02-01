package main;

import ip.mapreduce.imgmatch.ImgMatchDriver;
import ip.mapreduce.imgmatch.ImgMatchMapper;
import ip.mapreduce.sift2hbase.Sift2HbaseDriver;
import ip.sift.Feature;
import ip.sift.LSHForSIFT;
import ip.sift.SIFT;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.imageio.ImageIO;

import model.GlobalVars;
import model.HDFSImg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.util.ToolRunner;

import service.SIFTExtractService;
import tools.HDFSFileTools;
import tools.HbaseTableTools;

public class DistriImgPro {
	
	private static int spiltNum = 0;
	
	private static int mapredSlot = 20;

	public static void createImgSiftTable() throws Exception {

		String tablename = GlobalVars.TABLE_IMG_INFO;

		String[] familyArr = { GlobalVars.FAMILY_INFO };
		HbaseTableTools.creatTable(tablename, familyArr);

	}

	public static void createLSHImgTable() throws Exception {

		String tablename = GlobalVars.TABLE_LSH_IMG;

		String[] familyArr = { GlobalVars.FAMILY_DST_IMG };
		HbaseTableTools.creatTable(tablename, familyArr);

	}

	public static void createSimImgTable() throws Exception {

		String tablename = GlobalVars.TABLE_SIM_IMG;

		String[] familyArr = { GlobalVars.FAMILY_DST_IMG };
		HbaseTableTools.creatTable(tablename, familyArr);

	}

	public static void makeImgs2Seq() throws IOException {

		long start_time = System.currentTimeMillis();
		
		HDFSFileTools.deletAllFiles(GlobalVars.SEQ_IMG_DIR);

		String dirPath = GlobalVars.ROOT_DIR + File.separator + GlobalVars.INPUT_IMG_DIR;
		String dstFilePath = GlobalVars.SEQ_IMG_PATH;

		List<HDFSImg> hfList = new ArrayList<HDFSImg>();
		FileStatus[] fileStatusArray = GlobalVars.getFs().listStatus(new Path(dirPath));
		
		spiltNum = fileStatusArray.length / mapredSlot;
		int num = 0;
		for (FileStatus fileStatus : fileStatusArray) {
			// if it is a file
			if (GlobalVars.getFs().isFile(fileStatus.getPath())) {
				hfList.add(HDFSFileTools.readImgFile(fileStatus.getPath().toString()));
				if (hfList.size() == spiltNum) {
					HDFSFileTools.writeImg2SequenceFile(dstFilePath + String.valueOf(num++), hfList);
					hfList = new ArrayList<HDFSImg>();
				}

			}
		}

		if (hfList.size() != 0) {
			HDFSFileTools.writeImg2SequenceFile(dstFilePath + String.valueOf(num++), hfList);
		}
		
		System.out.println("makeImgs2Seq time:	" + (System.currentTimeMillis() - start_time) + "ms");

	}
	
	public static void getSimImg() throws Exception {

		long start_time = System.currentTimeMillis();
		String srcSeqImgDir = GlobalVars.QUERY_IMG_DIR;
		List<HDFSImg> imgList = HDFSFileTools.readImgDir(srcSeqImgDir);
		for (HDFSImg img : imgList) {

			InputStream in = new ByteArrayInputStream(img.getBw().getBytes());
			Vector<Feature> ftList = SIFT.getFeatures(ImageIO.read(in));
			Set<String> strSet = SIFTExtractService.getSimPath(ftList);

			Iterator<String> iter = strSet.iterator();
			int count = 0;
			while (iter.hasNext()) {
				String imgId = iter.next();
				Vector<Feature> dstList = SIFTExtractService.getImgFeatureByID(imgId);
				if(dstList != null){
					float sim = SIFT.getMatchRadio(ftList, dstList);
					if (sim > 0.7) {
						if(count++ > 100){break;}
						System.out.println(imgId + " = " + sim);
					}
				}
				
				
			}
			System.out.println("count = " + count);
		}
		
		System.out.println("match time:	" + (System.currentTimeMillis() - start_time) + "ms");
	}
	


	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// //create tables
//		 createImgSiftTable();
//		 createLSHImgTable();
//		 createSimImgTable();

		// make imgs into seqFile
//		makeImgs2Seq();
		
		// get sift feature into table
		 long start_time = System.currentTimeMillis();
		 int result = ToolRunner.run(new Configuration(), new
		 Sift2HbaseDriver(), args);
		 System.out.println("extract_sift time:	" +
		 (System.currentTimeMillis() - start_time) + "ms");

//		LSHForSIFT.bucketCnt = 16;
//		 getSimImg();
		
//		 long start_time = System.currentTimeMillis();
//		 String srcSeqImgDir = GlobalVars.QUERY_IMG_DIR;
//		 List<HDFSImg> imgList = HDFSFileTools.readImgDir(srcSeqImgDir);
//		 for (HDFSImg img : imgList) {
//		
//		 System.out.println(img.getFilename().toString());
//		
//		 InputStream in = new ByteArrayInputStream(img.getBw().getBytes());
//		 Vector<Feature> srcFVector = SIFT.getFeatures(ImageIO.read(in));
//		 GlobalVars.setSrcFVector(srcFVector);
//		 GlobalVars.setSrcPath(img.getFilename().toString());
//		 ToolRunner.run(new Configuration(), new ImgMatchDriver(), args);
//		 break;
//		
//		 }
//		 System.out.println("match time:	" + (System.currentTimeMillis() -
//		 start_time) + "ms");


	}

}
