package main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import utils.CharType;
import utils.DistanceCalculations;

public class WriteTestQS {
	
	private ConcurrentHashMap<String, Integer> gMapCompress;
	private ConcurrentHashMap<Integer, String> gMapDecompress;
	private ExecutorService gExecutorRequests;
	private int[] gCircularArray;
	private int gCirculararrayLen, gNumBitsPointer;
	private DistanceCalculations gDc;
	
	public WriteTestQS(String[] args) {
		gMapCompress = new ConcurrentHashMap<String, Integer>();
		gMapDecompress = new ConcurrentHashMap<Integer, String>();
		this.gDc = new DistanceCalculations(CharType.CD);

		BlockingQueue<Runnable> queue = new SynchronousQueue<>();
		gExecutorRequests = new ThreadPoolExecutor(24, 24, 500, TimeUnit.MILLISECONDS, queue,
				new ThreadPoolExecutor.CallerRunsPolicy());
		gCircularArray = new int[43];
		for (int i = 0; i < gCircularArray.length; i++) {
			gCircularArray[i] = i;
		}

		gCirculararrayLen = gCircularArray.length;
		loadCentroids(args[0]);

		gNumBitsPointer = Integer.toBinaryString(gMapCompress.size() - 1).length();
		getDistances(args[1]);
		try {
			gExecutorRequests.awaitTermination(5, TimeUnit.SECONDS);
			gExecutorRequests.shutdown();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void getDistances(String theQsFile) {
		try {
			RandomAccessFile aFile = new RandomAccessFile(theQsFile, "r");
			String aTextLine;
			while ((aTextLine = aFile.readLine()) != null) {
				gExecutorRequests.execute(new StringRunnable(aTextLine));
			}
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			aFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private class StringRunnable implements Runnable {
		String gString;

		public StringRunnable(String theString) {
			gString = theString;
		}

		@Override
		public void run() {
			// Convert QS line to circular delta values
			int[] aListDelta = new int[gString.length()];
			int aPrevChar = gString.charAt(0);
			int j = 0;
			StringBuilder sb = new StringBuilder();
			for (char c : gString.toCharArray()) {
				if (j > 0) {
					int i = c;
					aListDelta[j] = circularDistance(aPrevChar, i, gCirculararrayLen);
					sb.append((char) (aListDelta[j] + 75));
					aPrevChar = i;
				}
				j++;
			}
			gString = sb.toString();
			// Find the best candidate
			int aMinDistanceHam = Integer.MAX_VALUE;
//			String aBestCandidateHam = "";
			for (String aCandidate : gMapCompress.keySet()) {
				int aBitsDistance = gDc.bitsizeFixedHammingCircularDeltaHuffman(aCandidate, gString);
				if (aBitsDistance < aMinDistanceHam) {
					aMinDistanceHam = aBitsDistance;
//					aBestCandidateHam = aCandidate;
				}
			}
			
//			System.out.println(gString + " " + aBestCandidateHam + " " + aMinDistanceHam);
			System.out.println(gNumBitsPointer+gDc.getCharSize()+aMinDistanceHam);
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
			while ((aTextLine = aFile.readLine()) != null) {
				String[] aTokens = aTextLine.split(",");
				StringBuilder sb = new StringBuilder();
				for (String aToken : aTokens) {
					int aInt = Integer.parseInt(aToken);
					sb.append((char) (((int) aInt) + 33 + 42));
				}
				gMapCompress.put(sb.toString(), i);
				gMapDecompress.put(i, sb.toString());
			}
			aFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		new WriteTestQS(args);
	}

}
