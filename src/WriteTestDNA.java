package main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import utils.CharType;
import utils.DistanceCalculations;

public class WriteTestDNA {
	private ExecutorService gExecutorRequests;
	private int gNumBitsPointer;
	private DistanceCalculations gDc;

	public WriteTestDNA(String[] args) {
		this.gDc = new DistanceCalculations(CharType.DNA);
		BlockingQueue<Runnable> queue = new SynchronousQueue<>();
		gExecutorRequests = new ThreadPoolExecutor(24, 24, 500, TimeUnit.MILLISECONDS, queue,
				new ThreadPoolExecutor.CallerRunsPolicy());
		gNumBitsPointer = 32;
		getDistances(args[0]);
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
		String gLine;
		
		public StringRunnable(String theString) {
			gLine = theString;
		}
		
		@Override
		public void run() {
			String[] aTokens = gLine.split(" "); // query position reference
			int aBitsDistance = gDc.bitsizeDeltaHammingDnaHuffman(aTokens[2], aTokens[1]);
			System.out.println(gNumBitsPointer + aBitsDistance);
		}
	}

	public static void main(String[] args) {
		new WriteTestDNA(args);
	}

}
