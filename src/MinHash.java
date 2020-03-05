package utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class MinHash {
	protected final long MARSENNE_PRIME = (1l << 61) - 1;
	protected final long MAX_HASH = (1l << 32) - 1;
	protected final long HASH_RANGE = (1l << 32);

	private long[] gHashValues;
	private ArrayList<long[]> gPermutations;

	private MessageDigest gCleanCrypt;
	long hv;
	long[] a, b, phv;
	int gBitSampling;

	public MinHash(int theNumPermutations, long theSeed, int theBitSampling) {
		try {
			gCleanCrypt = MessageDigest.getInstance("SHA-1");
			gCleanCrypt.reset();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		gHashValues = new long[theNumPermutations];
		Arrays.fill(gHashValues, MAX_HASH);
		gPermutations = new ArrayList<long[]>();
		Random aRand = new Random(theSeed);
		this.gPermutations.add((aRand.longs(theNumPermutations, 1l, MARSENNE_PRIME)).toArray());
		this.gPermutations.add((aRand.longs(theNumPermutations, 0l, MARSENNE_PRIME)).toArray());
		gBitSampling = theBitSampling;
	}

	public int length() {
		return gHashValues.length;
	}

	public long[] digest() {
		return gHashValues.clone();
	}
	
	public int getBitSamplingSize()
	{
		return gBitSampling;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for (long l : digest()){
			sb.append(l+", ");
		}
		return sb.toString();
	}

	public void update(String theToken) {
		try {
//			long t1 = Timer.ns();
			MessageDigest aCrypt = (MessageDigest) gCleanCrypt.clone();
//			long t2 = Timer.ns();
//			long t3 = Timer.ns();
			aCrypt.update(theToken.getBytes("UTF-8"));
//			long t4 = Timer.ns();
			hv = Integer.toUnsignedLong(ByteBuffer.wrap(Arrays.copyOfRange(aCrypt.digest(), 0, 4)).getInt());
//			long t5 = Timer.ns();
//			System.out.println("hv: "+hv);
			a = gPermutations.get(0);
			b = gPermutations.get(1);
			phv = new long[a.length];
//			long t6 = Timer.ns();
			for (int i = 0; i < phv.length; i++) {
				phv[i] = ((a[i] * hv + b[i]) % MARSENNE_PRIME) & MAX_HASH;
				gHashValues[i] = Math.min(phv[i], gHashValues[i]);
			}
//			long t7 = Timer.ns();
//			System.out.println((t2-t1)+" "+(t3-t2)+" "+(t4-t3)+" "+(t5-t4)+" "+(t6-t5)+" "+(t7-t6));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
	}

	public float jaccard(MinHash theOtherHash) {
		long[] theSelf = digest();
		long[] theOther = theOtherHash.digest();
		if (theSelf.length != theOther.length) {
			return 0.0f;
		}
		float intersection = 0;
		for (int i = 0; i < theSelf.length; i++) {
			if (theSelf[i] == theOther[i]) {
				intersection++;
			}
		}
		return intersection / theSelf.length;
	}

	public float bitSamplingJaccard(MinHash theOtherHash) {
		int[] theSelf = bitSampling();
		int[] theOther = theOtherHash.bitSampling();
		if (theSelf.length != theOther.length) {
			System.out.println("Different bitsampling length");
			return 0.0f;
		}
		// intersection = np.count_nonzero(self.hashvalues==other.hashvalues)
		float intersection = 0;
		for (int i = 0; i < theSelf.length; i++) {
			if (theSelf[i] == theOther[i]) {
				intersection++;
			}
		}
//		System.out.println("intersection: "+intersection+" out of "+ theSelf.length);
		// raw_est = float(intersection) / float(self.hashvalues.size)
		float raw_est = intersection / theSelf.length;
//		System.out.println("raw_est: "+raw_est);
		float a1, a2, r1, r2;
		Tuple<Float, Float> aTuple;
		r1 = (float) (length() / Math.pow(93, 4));
		r2 = (float) (theOtherHash.length() / Math.pow(93, 4));
		System.out.println(r1+" "+ r2);
		a1 = calc_a(r1, gBitSampling);
		a2 = calc_a(r2, gBitSampling);
		aTuple = calc_c(a1, a2, r1, r2);
		// a1 = self._calc_a(self.r, self.b)
		// a2 = self._calc_a(other.r, other.b)
		// c1, c2 = self._calc_c(a1, a2, self.r, other.r)
		return (raw_est - aTuple.getFirst()) / (1 - aTuple.getSecond());
	}

	private Tuple<Float, Float> calc_c(float a1, float a2, float r1, float r2) {
		if (r1 == 0.0f && r2 == 0.0f) {
			return new Tuple<Float, Float>(a1, a2);
		}
		float div = 1f / (r1 + r2);
		float c1 = (a1 * r2 + a2 * r1) * div;
		float c2 = (a1 * r1 + a2 * r2) * div;
		return new Tuple<Float, Float>(c1, c2);
	}

	private float calc_a(float r, int b) {
		if (r == 0.0f) {
			// Find the limit of A(r, b) as r -> 0.
			return 1.0f / (1 << b);
		}
		return (float) (   (  r * Math.pow((1 - r), (Math.pow(2, b) - 1))  ) / (  1 - Math.pow((1 - r), Math.pow(2, b))  )   );
	}

	public int[] bitSampling() {
		int[] aBitSampling = new int[gHashValues.length];
//		System.out.println("bitsampling size: "+gHashValues.length);
		long bmask = (1 << gBitSampling) - 1;
		for (int i = 0; i < gHashValues.length; i++) {
			aBitSampling[i] = (int) (gHashValues[i] & bmask);
		}
		return aBitSampling;
	}
}
