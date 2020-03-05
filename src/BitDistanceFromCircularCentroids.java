package main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import utils.CharType;
import utils.AggregateLevenshtein;
import utils.DistanceCalculations;
import utils.EditOperation;
import utils.Levenshtein;
import utils.Timer;

public class BitDistanceFromCircularCentroids {
	ConcurrentHashMap<String, Integer> gMapCompress;
	ConcurrentHashMap<Integer, String> gMapDecompress;

	private AggregateLevenshtein gAL;
	private Levenshtein gL;
	private DistanceCalculations gDc;
	private ExecutorService gExecutorRequests;
	private int gNumBitsPointer;
	private long gTimeHamming, gTimeLevenshtein, gTimeDeltaHamming;

	int[] gCircularArray;
	int gCirculararrayLen;

	public BitDistanceFromCircularCentroids(String[] args) {
		gMapCompress = new ConcurrentHashMap<String, Integer>();
		gMapDecompress = new ConcurrentHashMap<Integer, String>();
		gAL = new AggregateLevenshtein();
		gL = new Levenshtein();
		gTimeHamming = 0;
		gTimeLevenshtein=0;
		gTimeDeltaHamming=0;
		this.gDc = new DistanceCalculations(CharType.CD);
		BlockingQueue<Runnable> queue = new SynchronousQueue<>();
		gExecutorRequests = new ThreadPoolExecutor(48, 48, 500, TimeUnit.MILLISECONDS, queue,
				new ThreadPoolExecutor.CallerRunsPolicy());
		gCircularArray = new int[43];
		for (int i = 0; i < gCircularArray.length; i++) {
			gCircularArray[i] = i;
		}

		gCirculararrayLen = gCircularArray.length;
		loadCentroids(args[0]);
		System.out.println("Pointer size: "+gMapCompress.size()+" "+Integer.toBinaryString(gMapCompress.size()).length());
		gNumBitsPointer = Integer.toBinaryString(gMapCompress.size()-1).length();
		getDistances(args[1]);
		try {
			gExecutorRequests.awaitTermination(5, TimeUnit.SECONDS);
			gExecutorRequests.shutdown();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	private synchronized void incrementTimeHamming(long theTime){
		gTimeHamming += theTime;
	}
	
	private synchronized void incrementTimeLevenshtein(long theTime){
		gTimeLevenshtein += theTime;
	}
	
	private synchronized void incrementTimeDeltaHamming(long theTime){
		gTimeDeltaHamming += theTime;
	}

	private void getDistances(String theFastqFile) {
		try {
			RandomAccessFile aFile = new RandomAccessFile(theFastqFile, "r");
			String aTextLine;
			int i = 1;
			int j = 0;
//			System.out.println("Starting to compress entries");
			while ((aTextLine = aFile.readLine()) != null) {
				if (i % 4 == 0) {
					gExecutorRequests.execute(new StringRunnable(aTextLine));
					i = 0;
					j++;
				}
				i++;
			}
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("[Hamming] time to compare "+j+" reads against "+gMapCompress.size()+" centroids = "+gTimeHamming+" ns ("+(gTimeHamming/j)+" ns/read)");
			System.out.println("[Levenshtein] time to compare "+j+" reads against "+gMapCompress.size()+" centroids = "+gTimeLevenshtein+" ns ("+(gTimeLevenshtein/j)+" ns/read)");
			System.out.println("[DeltaHamming] time to compare "+j+" reads against "+gMapCompress.size()+" centroids = "+gTimeDeltaHamming+" ns ("+(gTimeDeltaHamming/j)+" ns/read)");
//			System.out.println("Finished to compress entries");
			aFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private class StringRunnable implements Runnable {
		String gString;
		int gHash;

		public StringRunnable(String theString) {
			gString = theString;
			gHash = gString.hashCode();

		}

		@Override
		public void run() {
			int[] aListDelta = new int[gString.length()];
			int aPrevChar = gString.charAt(0);
			int j = 0;
			StringBuilder sb = new StringBuilder();
			for (char c : gString.toCharArray()) {
				if (j > 0) {
					int i = c;
					aListDelta[j] = circularDistance(aPrevChar, i, gCirculararrayLen);
					sb.append((char) (aListDelta[j]+75));
					aPrevChar = i;
				}
				j++;
			}
			gString = sb.toString();
//			System.out.println("Comressing: "+gString);
			
			int aMinDistanceHam = Integer.MAX_VALUE;
			int aMinDistanceLev = Integer.MAX_VALUE;
			int aMinDistanceDelta = Integer.MAX_VALUE;
			String aBestCandidateHam = "";
			String aBestCandidateLev = "";
			String aBestCandidateDelta = "";
			long t1, t2;
			for (String aCandidate : gMapCompress.keySet()) {
				t1 = Timer.ns();
				ArrayList<EditOperation> ops = gL.getStandardEditOperationsWithNoOp(aCandidate, gString);
				ArrayList<EditOperation> sdops = gL.getSingleDeltaEditOperationsWithNoOp(ops, gString);
				
				int aBitsDistance = gDc.sizeFixedLevOpsHufChar(sdops);
				if (aBitsDistance < aMinDistanceLev) {
					aMinDistanceLev = aBitsDistance;
					aBestCandidateLev = aCandidate;
				}
				t2 = Timer.ns();
				incrementTimeLevenshtein((t2-t1));
				
				t1 = Timer.ns();
				aBitsDistance = gDc.sizeFixedHamOpsHufChar(aCandidate, gString);
				if (aBitsDistance < aMinDistanceHam) {
					aMinDistanceHam = aBitsDistance;
					aBestCandidateHam = aCandidate;
				}
				t2 = Timer.ns();
				incrementTimeHamming((t2-t1));
				
				t1 = Timer.ns();
				aBitsDistance = gDc.calculateDeltaHammingCDDistance(aCandidate, gString);
				if (aBitsDistance < aMinDistanceDelta) {
					aMinDistanceDelta = aBitsDistance;
					aBestCandidateDelta = aCandidate;
				}
				t2 = Timer.ns();
				incrementTimeDeltaHamming((t2-t1));
			}
				
//			System.out.println(gString+" "+aMinDistanceHam + " " + aMinDistanceLev + " " + aMinDistanceDelta+" "+gDc.calculateDeltaHammingCDString(aBestCandidateDelta, gString) + " " + aBestCandidateDelta);
			System.out.println(aMinDistanceLev+" "+aMinDistanceHam + " " + aMinDistanceDelta);
		}
	}

	public int circularDistance(int theSrc, int theDst, int theLen) {
		int dm = Math.floorMod((theDst - theSrc), theLen);
		if (dm < (theLen / 2)) {
			return dm;
		}
		return dm - theLen;
	}

	private void loadCentroids(String theCentroidsFile) {
		try {
			RandomAccessFile aFile = new RandomAccessFile(theCentroidsFile, "r");
			String aTextLine;
			int i = 0;
			long t1 = Timer.ns();
//			System.out.println("Starting to load centroids");
			while ((aTextLine = aFile.readLine()) != null) {
				String[] aTokens = aTextLine.split(",");
				StringBuilder sb = new StringBuilder();
				for (String aToken : aTokens) {
					int aInt = Integer.parseInt(aToken);
					sb.append((char) (((int) aInt) + 33 + 42));
				}
//				System.out.println("Centroid: " + sb.toString());
				gMapCompress.put(sb.toString(), i);
				gMapDecompress.put(i, sb.toString());
			}
//			System.out.println("Finishing to load centroids");
			long t2 = Timer.ns();
//			System.out.println("Time to load "+gMapCompress.size()+" centroids = "+(t2-t1)+" ns ("+(gMapCompress.size()/((t2-t1)/1_000_000_000))+" ops/s)");
			System.out.println("Time to load "+gMapCompress.size()+" centroids = "+(t2-t1)+" ns)");

			aFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		new BitDistanceFromCircularCentroids(args);
	}

}
