package main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CircularDeltaFromFastq {

	private ExecutorService gExecutorRequests;
	int[] gCircularArray;
	int gCircularArrayLen;

	public CircularDeltaFromFastq(String[] args) {
		gCircularArray = new int[43];
		for (int i = 0; i < gCircularArray.length; i++) {
			gCircularArray[i] = i;
		}
		gCircularArrayLen = gCircularArray.length;
		System.out.println("@RELATION circulardelta");
		for (int i = 1; i <= 99; i++) {
			System.out.println("@ATTRIBUTE d" + i + " NUMERIC");
		}
		System.out.println("@DATA");
		try {
			BlockingQueue<Runnable> queue = new SynchronousQueue<>();
			ExecutorService gExecutorRequests = new ThreadPoolExecutor(48, 48, 500, TimeUnit.MILLISECONDS, queue,
					new ThreadPoolExecutor.CallerRunsPolicy());
			RandomAccessFile aFile = new RandomAccessFile(args[0], "r");
			String aTextLine;
			int i = 1;
			while ((aTextLine = aFile.readLine()) != null) {
				if (i % 4 == 0) {
					gExecutorRequests.execute(new StringRunnable(aTextLine));
					i = 0;
				}
				i++;
			}
			aFile.close();
			gExecutorRequests.awaitTermination(5, TimeUnit.SECONDS);
			gExecutorRequests.shutdown();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
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
			int[] aListDelta = new int[gString.length()];
			int aPrevChar = gString.charAt(0);
			int j = 0;
			for (char c : gString.toCharArray()) {
				if (j > 0) {
					int i = c;
					aListDelta[j] = circularDistance(aPrevChar, i, gCircularArrayLen);
					aPrevChar = i;
				}
				j++;
			}
			StringBuilder sb = new StringBuilder();
			j = 0;
			for (int i : aListDelta) {
				if (j > 0) {
					// sb.append((char) (i + 75));
					sb.append(i + ",");
				}
				j++;
			}
			if (sb.length() > 0) {
				sb.deleteCharAt(sb.length() - 1);
			}
			System.out.println(sb.toString());
		}

		public int circularDistance(int theSrc, int theDst, int theLen) {
			int dm = Math.floorMod((theDst - theSrc), theLen);
			if (dm < (theLen / 2)) {
				return dm;
			}
			return dm - theLen;
		}
	}

	public static void main(String[] args) {
		new CircularDeltaFromFastq(args);
	}

}
