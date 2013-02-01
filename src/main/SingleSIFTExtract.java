package main;

import ip.sift.Feature;
import ip.sift.LSHForSIFT;
import ip.sift.SIFT;

import java.awt.image.BufferedImage;
import java.io.File;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.imageio.ImageIO;


public class SingleSIFTExtract {

	public static int runThreadNum = 0;
	
	public synchronized static void addThreadNum(int i){
		runThreadNum += i;
	}
	
	public synchronized static void reduceThreadNum(int i){
		runThreadNum -= i;
	}
	
	public static boolean isAllThreadFinished(){
		return runThreadNum == 0;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		long start_time = System.currentTimeMillis();
		
//		String dirStr = args[1];
		final String dirStr = "../srcImg";
		File dir = new File(dirStr);
		String[] filelist = dir.list();
		int poolTotal = 10;
		ExecutorService pool = Executors.newFixedThreadPool(poolTotal);
		
		for (final String file:filelist){
			pool.execute(new Thread(){
				public void run(){
					try {
						addThreadNum(1);
						BufferedImage img1 = ImageIO.read(new File(dirStr + File.separator + file));
						Vector<Feature> fs1 = SIFT.getFeatures(img1);
						Set<String> lshSet = LSHForSIFT.getLSH(fs1);
//						System.out.println(lshSet.size());
						reduceThreadNum(1);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			
		}
		
		while(!isAllThreadFinished()){
			try {
				Thread.sleep(1000);
//				System.out.println("wait 1s " + runThreadNum);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("total " + (System.currentTimeMillis() - start_time) + "ms");
		pool.shutdown();
	}

}
