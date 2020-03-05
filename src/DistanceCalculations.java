package utils;

import java.util.ArrayList;
import java.util.BitSet;

import utils.CharType;
import utils.EditOperation;
import utils.EditType;

public class DistanceCalculations {
	private final String[] gHuffmanCircularDeltas = new String[] { "0101100110", "0101101101", "0101101110",
			"0101101111", "0110101110", "0111001101", "010001011", "011010110", "010110010", "011100111", "01000100",
			"01101001", "01101000", "0100110", "0100101", "010000", "000110", "00010", "011011", "01100", "0000",
			"0011", "1", "0010", "01111", "01010", "010111", "011101", "0111000", "000111", "0100100", "0100111",
			"01110010", "01101010", "01011010", "01000111", "01000110", "01011000", "010001010", "0111001100",
			"0110101111", "0101101100", "0101100111" };
	private int[] gHuffmanQualityScores, gHuffmanNormalDelta, gHuffmanCircularDelta, gHuffmanDna, gAppropriateHuffman;
	private int gCharSize;
	private int gHuffmanOffset;
	private CharType gCharType;

	public DistanceCalculations(CharType theCharType) {
		// gHuffmanQualityScores: [33,75] = [0,42] = (43)
		gHuffmanQualityScores = new int[] { 15, 14, 2, 13, 13, 12, 11, 10, 10, 10, 9, 9, 10, 10, 9, 9, 9, 8, 8, 8, 8, 8,
				7, 7, 8, 7, 7, 7, 6, 6, 6, 6, 6, 5, 6, 4, 5, 6, 3, 4, 2, 14, 15 };
		// gHuffmanNormalDelta: [-42,42] = [0,84] = (85)
		gHuffmanNormalDelta = new int[] { 34, 33, 32, 31, 16, 16, 12, 12, 11, 11, 12, 9, 10, 10, 9, 13, 12, 12, 12, 11,
				11, 11, 10, 10, 10, 10, 9, 9, 9, 8, 8, 8, 8, 7, 7, 6, 7, 5, 6, 5, 4, 4, 1, 4, 5, 5, 6, 6, 7, 6, 7, 7, 8,
				8, 8, 8, 9, 9, 9, 10, 10, 10, 10, 11, 11, 12, 12, 13, 13, 14, 15, 17, 18, 19, 20, 21, 30, 29, 28, 27,
				26, 25, 24, 23, 22 };
		// gHuffmanCircularDelta: [-22,20] = [0,42] = (43)
		gHuffmanCircularDelta = new int[] { 10, 10, 10, 10, 10, 10, 9, 9, 9, 9, 8, 8, 8, 7, 7, 6, 6, 5, 6, 5, 4, 4, 1,
				4, 5, 5, 6, 6, 7, 6, 7, 7, 8, 8, 8, 8, 8, 8, 9, 10, 10, 10, 10 };
		gHuffmanDna = new int[] { 3, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 2 }; // A-T with values for
																								// A, C, G, N, and T
		// A=011, C=10, G=11, T=00, N=010
		gCharType = theCharType;
		switch (gCharType) {
		case QS:
			gCharSize = 6;
			gAppropriateHuffman = gHuffmanQualityScores;
			gHuffmanOffset = -33;
			break;
		case ND:
			gCharSize = 7;
			gAppropriateHuffman = gHuffmanNormalDelta;
			gHuffmanOffset = -33;
			break;
		case CD:
			gCharSize = 6;
			gAppropriateHuffman = gHuffmanCircularDelta;
			gHuffmanOffset = -53;
			break;
		case DNA:
			gCharSize = 3;
			gAppropriateHuffman = gHuffmanDna;
			gHuffmanOffset = -65; // A = 65 in ASCII
			break;
		default:
			System.out.println("[ERR] Unknown CharType " + gCharType);
			System.exit(1);
		}
	}

	public int getCharSize() {
		return gCharSize;
	}

	public int bitsizeDeltaHammingDnaHuffman(String theCandidate, String theOriginal) {
		int aDistance = 0;
		char[] aCandidate = theCandidate.toCharArray();
		char[] aOriginal = theOriginal.toCharArray();
		int aDelta = 0;
//		StringBuilder aSb = new StringBuilder(); //DEBUG
		if (aCandidate.length <= aOriginal.length) {
			for (int i = 0; i < aCandidate.length; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					aDelta++;
//					aSame++;
				} else {
//					aDiff++;
					if (aDelta != 0) {
						aDistance += 1; // 0 for delta
//						aSb.append(Integer.toString(aDelta)); //DEBUG
						aDistance += Integer.toString(aDelta).length() * 4; // e.g., 0=0000, 1=0001, ..., 7=0111,
																			// 8=1000, 9=1001
						aDelta = 0;
					}
					aDistance += 1; // 1 for char
					aDistance += getBitsHuffman(aOriginal[i]);
				}
			}
		}
		aDistance += 5; //0 1111 for end of operations
//		System.out.println("[DEBUG] "+theOriginal + " "+ theCandidate + " " + aDistance + " " + aSb.toString()); //DEBUG
//		System.out.println(aSame+" "+aDiff);
		return aDistance;
	}

	// ##### FIXED LEVENSHTEIN OPS #####
	// Original Zhou with 6-bit char (QS only)
	public int sizeFixedLevOpsFixedChar(ArrayList<EditOperation> theOps) {
		int aSize = 0;
		for (EditOperation e : theOps) {
			switch (e.type) {
			case NO_EDIT:
				aSize += 2;
				break;
			case DELETE_1:
				aSize += 2;
				break;
			case INSERT_1:
				aSize += 2 + gCharSize;
				break;
			case REPLACE_1:
				aSize += 2 + gCharSize;
				break;
			}
		}
		return aSize;
	}

	public int sizeFixedLevOpsVarChar(ArrayList<EditOperation> theOps) {
		int aSize = 0;
		aSize += 7; // space to store the number of bits required for the delta
					// on each operation
		int aNumBits = getNumBits(theOps);
		for (EditOperation e : theOps) {
			switch (e.type) {
			case NO_EDIT:
				aSize += 2;
				break;
			case DELETE_1:
				aSize += 2;
				break;
			case INSERT_1:
				aSize += 2 + aNumBits;
				break;
			case REPLACE_1:
				aSize += 2 + aNumBits;
				break;
			}
		}
		return aSize;
	}

	public int sizeFixedLevOpsHufChar(ArrayList<EditOperation> theOps) {
		int aSize = 0;
		for (EditOperation e : theOps) {
			switch (e.type) {
			case NO_EDIT:
				aSize += 2;
				break;
			case DELETE_1:
				aSize += 2;
				break;
			case INSERT_1:
				aSize += 2 + getBitsHuffman((int) e.c[0]);
				break;
			case REPLACE_1:
				aSize += 2 + getBitsHuffman((int) e.c[0]);
				break;
			}
		}
		return aSize;
	}

	// ##### HUFFMAN LEVENSHTEIN OPS #####
	public int sizeHufLevOpsFixedChar(ArrayList<EditOperation> theOps) {
		int aSize = 0;
		for (EditOperation e : theOps) {
			switch (e.type) {
			case NO_EDIT:
				aSize += 1;
				break;
			case DELETE_1:
				aSize += 3;
				break;
			case INSERT_1:
				aSize += 3 + gCharSize;
				break;
			case REPLACE_1:
				aSize += 2 + gCharSize;
				break;
			}
		}
		return aSize;
	}

	public int sizeHufLevOpsVarChar(ArrayList<EditOperation> theOps) {
		int aSize = 0;
		aSize += 7; // space to store the number of bits required for the delta
		// on each operation
		int aNumBits = getNumBits(theOps);
		for (EditOperation e : theOps) {
			switch (e.type) {
			case NO_EDIT:
				aSize += 1;
				break;
			case DELETE_1:
				aSize += 3;
				break;
			case INSERT_1:
				aSize += 3 + aNumBits;
				break;
			case REPLACE_1:
				aSize += 2 + aNumBits;
				break;
			}
		}
		return aSize;
	}

	public int sizeHufLevOpsHufChar(ArrayList<EditOperation> theOps) {
		int aSize = 0;
		for (EditOperation e : theOps) {
			switch (e.type) {
			case NO_EDIT:
				aSize += 1;
				break;
			case DELETE_1:
				aSize += 3;
				break;
			case INSERT_1:
				aSize += 3 + getBitsHuffman((int) e.c[0]);
				break;
			case REPLACE_1:
				aSize += 2 + getBitsHuffman((int) e.c[0]);
				break;
			}
		}
		return aSize;
	}

	public int sizeHufLevOpsHufCharCommonPrefixSuffix(ArrayList<EditOperation> theOps) {
		int aSize = 0;
		aSize += 12; // space for the prefix and suffix size
		boolean isPrefix = true;
		int aSufixSize = 0;
		int i = 0;
		for (EditOperation e : theOps) {
			switch (e.type) {
			case NO_EDIT:
				if (i >= theOps.size() / 2) {
					isPrefix = false;
				}
				if (!isPrefix) {
					aSize += 1;
				}
				break;
			case DELETE_1:
				aSize += 3;
				break;
			case INSERT_1:
				aSize += 3 + getBitsHuffman((int) e.c[0]);
				break;
			case REPLACE_1:
				aSize += 2 + getBitsHuffman((int) e.c[0]);
				break;
			}
			i++;
		}
		for (i = theOps.size() - 1; i >= theOps.size() / 2; i--) {
			if (theOps.get(i).type == EditType.NO_EDIT) {
				aSufixSize++;
			} else {
				break;
			}
		}
		return aSize - aSufixSize;
	}

	// ##### FIXED HAMMING OPS #####
	public int sizeFixedHamOpsFixedChar(String theCandidate, String theOriginal) {
		char[] aCandidate = theCandidate.toCharArray();
		char[] aOriginal = theOriginal.toCharArray();
		int aDistance = 0;
		if (theCandidate.equals(theOriginal)) {
			return aDistance;
		}
		if (aCandidate.length <= aOriginal.length) {
			for (int i = 0; i < aCandidate.length; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					aDistance++;
				} else {
					aDistance += 1 + gCharSize;
				}
			}
		}
		return aDistance;
	}

	public int sizeFixedHamOpsFixedDnaChar(String theCandidate, String theOriginal) {
		char[] aCandidate = theCandidate.toCharArray();
		char[] aOriginal = theOriginal.toCharArray();
		int aDistance = 0;
		if (theCandidate.equals(theOriginal)) {
			return aDistance;
		}
		if (aCandidate.length <= aOriginal.length) {
			for (int i = 0; i < aCandidate.length; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					aDistance++;
				} else {
					aDistance += 1 + 2; // 3 bits for A, C, G, T
					// aDistance += 1 + 3; // 3 bits for A, C, G, T, N
				}
			}
		}
		return aDistance;
	}

	public int sizeFixedHamOpsVarChar(String theCandidate, String theOriginal) {
		char[] aCandidate = theCandidate.toCharArray();
		char[] aOriginal = theOriginal.toCharArray();
		int aDistance = 0;
		if (theCandidate.equals(theOriginal)) {
			return aDistance;
		}
		int aNumBits = getNumBits(theOriginal);
		aDistance += 7; // space to store the number of bits required for the
						// delta
		// on each operation
		if (aCandidate.length <= aOriginal.length) {
			for (int i = 0; i < aCandidate.length; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					aDistance++;
				} else {
					aDistance += 1 + aNumBits;
				}
			}
		}
		return aDistance;
	}

	public int bitsizeFixedHammingCircularDeltaHuffman(String theCandidate, String theOriginal) {
		char[] aCandidate = theCandidate.toCharArray();
		char[] aOriginal = theOriginal.toCharArray();
//		StringBuilder aSb = new StringBuilder(); //DEBUG
		int aDistance = 0;
		if (aCandidate.length <= aOriginal.length) {
			for (int i = 0; i < aCandidate.length; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					aDistance++;
//					aSb.append("U"); //DEBUG
				} else {
					aDistance += (1 + getBitsHuffman(aOriginal[i]));
//					aSb.append("S"); //DEBUG
//					aSb.append(aOriginal[i]); //DEBUG
				}
			}
		}
//		System.out.println("[DEBUG] "+theOriginal + " "+ theCandidate + " " + aDistance + " " + aSb.toString()); //DEBUG
		return aDistance;
	}

	public int sizeFixedHamOpsHufCharCommonPrefixSufix(String theCandidate, String theOriginal) {
		char[] aCandidate = theCandidate.toCharArray();
		char[] aOriginal = theOriginal.toCharArray();
		int aDistance = 0;
		aDistance += 18; // space for the prefix and suffix size, and first char
		int aSizeSuffix = 0;
		if (theCandidate.equals(theOriginal)) {
			return aDistance;
		}
		if (aCandidate.length <= aOriginal.length) {
			int aSizePrefix = 0;
			for (int i = 0; i < aCandidate.length; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					aSizePrefix++;
				} else {
					break;
				}
			}
			// suffix (6 bits)
			aSizeSuffix = 0;
			for (int i = aCandidate.length - 1; i > 0; i--) {
				if (aCandidate[i] == aOriginal[i]) {
					aSizeSuffix++;
				} else {
					break;
				}
			}
			for (int i = aSizePrefix; i < aCandidate.length - aSizeSuffix; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					aDistance++;
				} else {
					aDistance += (1 + getBitsHuffman(aOriginal[i]));
				}
			}

		}
		return aDistance;
	}

	public int sizeFixedHamOpsFixDnaCharCommonPrefixSufix(String theCandidate, String theOriginal) {
		char[] aCandidate = theCandidate.toCharArray();
		char[] aOriginal = theOriginal.toCharArray();
		int aDistance = 0;
		aDistance += 12; // space for the prefix, suffix size, and first char
		int aSizeSuffix = 0;
		if (theCandidate.equals(theOriginal)) {
			return aDistance;
		}
		if (aCandidate.length <= aOriginal.length) {
			int aSizePrefix = 0;
			for (int i = 0; i < aCandidate.length; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					aSizePrefix++;
				} else {
					break;
				}
			}
			// suffix (6 bits)
			aSizeSuffix = 0;
			for (int i = aCandidate.length - 1; i > 0; i--) {
				if (aCandidate[i] == aOriginal[i]) {
					aSizeSuffix++;
				} else {
					break;
				}
			}
			// System.out.println("pre: " + aSizePrefix + " suf: " +
			// aSizeSuffix);
			for (int i = aSizePrefix; i < aCandidate.length - aSizeSuffix; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					aDistance++;
				} else {
					aDistance++;
					aDistance += 2;
				}
			}
		}
		return aDistance;
	}

	private int getBitsHuffmanDna(char theC) {
		switch (theC) {
		case 'A':
			return 3;
		case 'C':
			return 2;
		case 'G':
			return 2;
		case 'T':
			return 2;
		default:
			return 3;
		}
	}

	public int getBitsHuffman(int theDelta) {
		return gAppropriateHuffman[theDelta + gHuffmanOffset];
	}

	private int getNumBits(String theOriginal) {
		if (gCharType == CharType.QS) {
			return 6;
		}
		int aSmallestC = 0, aBiggestC = 0;
		int aChar;
		for (char c : theOriginal.toCharArray()) {
			aChar = (int) ((int) c - 75);
			if (aChar < aSmallestC) {
				aSmallestC = aChar;
			}
			if (aChar > aBiggestC) {
				aBiggestC = aChar;
			}
		}
		// aSmallestC < 0 and aBiggestC > 0
		if ((aBiggestC + aSmallestC) > 0) {
			if (aBiggestC > 32) {
				return 7;
			} else if (aBiggestC > 16) {
				return 6;
			} else if (aBiggestC > 8) {
				return 5;
			} else {
				return 4;
			}
			// aBiggestC matters
		} else {
			// aSmallestC matters
			if (aSmallestC < -32) {
				return 7;
			} else if (aSmallestC < -16) {
				return 6;
			} else if (aSmallestC < -8) {
				return 5;
			} else {
				return 4;
			}
		}
	}

	private int getNumBits(ArrayList<EditOperation> theOps) {
		if (gCharType == CharType.QS) {
			return 6;
		}
		int aSmallestC = 0, aBiggestC = 0;
		int aChar;
		for (EditOperation e : theOps) {
			if (e.type == EditType.INSERT_1 || e.type == EditType.REPLACE_1) {
				aChar = (int) ((int) e.c[0] - 75);
				if (aChar < aSmallestC) {
					aSmallestC = aChar;
				}
				if (aChar > aBiggestC) {
					aBiggestC = aChar;
				}
			}
		}
		// aSmallestC < 0 and aBiggestC > 0
		if ((aBiggestC + aSmallestC) > 0) {
			// aBiggestC matters
			if (aBiggestC > 32) {
				return 7;
			} else if (aBiggestC > 16) {
				return 6;
			} else if (aBiggestC > 8) {
				return 5;
			} else {
				return 4;
			}
		} else {
			// aSmallestC matters
			if (aSmallestC < -32) {
				return 7;
			} else if (aSmallestC < -16) {
				return 6;
			} else if (aSmallestC < -8) {
				return 5;
			} else {
				return 4;
			}
		}
	}

	public int calculateDeltaHammingDnaDistance(String theCandidate, String theOriginal) {
		int aDistance = 0;
		char[] aCandidate = theCandidate.toCharArray();
		char[] aOriginal = theOriginal.toCharArray();
		if (aCandidate.length <= aOriginal.length) {
			int aDelta = 0;
			for (int i = 0; i < aCandidate.length; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					aDelta++;
				} else {
					if (aDelta != 0) {
						aDistance += Integer.toString(aDelta).length() * 4;
						aDelta = 0;
					}
					switch (aOriginal[i]) {
					case 'C': // 10
					case 'G': // 11
					case 'T': // 00
						aDistance += 2;
						break;
					case 'A': // 011
					case 'N': // 010
						aDistance += 3;
						break;
					}
					// 92AGATCGG
				}
			}
		}
		return aDistance;
	}

	public int calculateDeltaHammingCDDistance(String theCandidate, String theOriginal) {
		int aDistance = 0;
		char[] aCandidate = theCandidate.toCharArray();
		char[] aOriginal = theOriginal.toCharArray();
		if (aCandidate.length <= aOriginal.length) {
			int aDelta = 0;
			for (int i = 0; i < aCandidate.length; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					aDelta++;
				} else {
					if (aDelta != 0) {
						aDistance += Integer.toString(aDelta).length() * 5;
						aDelta = 0;
					}
					// System.out.println(aOriginal[i]+"
					// "+getBitsHuffman(aOriginal[i]));
					aDistance += 1 + getBitsHuffman(aOriginal[i]);
				}
			}
		}
		return aDistance;
	}

	public String calculateDeltaHammingCDString(String theCandidate, String theOriginal) {
		int aDistance = 0;
		StringBuilder aSb = new StringBuilder();
		char[] aCandidate = theCandidate.toCharArray();
		char[] aOriginal = theOriginal.toCharArray();
		if (aCandidate.length <= aOriginal.length) {
			int aDelta = 0;
			for (int i = 0; i < aCandidate.length; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					aDelta++;
				} else {
					if (aDelta == 0) {
						aSb.append(aOriginal[i]);
					} else {
						aSb.append(aDelta + "" + aOriginal[i]);
						aDelta = 0;
					}

				}
			}
		}
		// System.out.println("edits: " + aSb.toString());
		return aSb.toString();
	}

	public BitSet calculateDeltaHammingCDBitSet(String theCandidate, String theOriginal) {
		BitSet bs = new BitSet();
		int aPos = 0;
		char[] aCandidate = theCandidate.toCharArray();
		char[] aOriginal = theOriginal.toCharArray();
		if (aCandidate.length <= aOriginal.length) {
			int aDelta = 0;
			for (int i = 0; i < aCandidate.length; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					aDelta++;
				} else {
					if (aDelta != 0) {
						// insert delta chars in 5 bits each number digit from delta
//						System.out.println("delta: "+aDelta);
						for (char c : Integer.toString(aDelta).toCharArray()) {
//							System.out.println(c);
							bs.clear(aPos); // 0 to indicate delta
							aPos++;
							switch (c) {
							case '0':
								aPos += 4;
								break;
							case '1':
								aPos += 3;
								bs.set(aPos);
								aPos++;
								break;
							case '2':
								aPos += 2;
								bs.set(aPos);
								aPos += 2;
								break;
							case '3':
								aPos += 2;
								bs.set(aPos);
								aPos++;
								bs.set(aPos);
								aPos++;
								break;
							case '4':
								aPos++;
								bs.set(aPos);
								aPos += 3;
								break;
							case '5':
								aPos++;
								bs.set(aPos);
								aPos += 2;
								bs.set(aPos);
								aPos++;
								break;
							case '6':
								aPos++;
								bs.set(aPos);
								aPos++;
								bs.set(aPos);
								aPos += 2;
								break;
							case '7':
								aPos++;
								bs.set(aPos);
								aPos++;
								bs.set(aPos);
								aPos++;
								bs.set(aPos);
								aPos++;
								break;
							case '8':
								bs.set(aPos);
								aPos += 4;
								break;
							case '9':
								bs.set(aPos);
								aPos += 3;
								bs.set(aPos);
								aPos++;
								break;
							}
						}
					}
					bs.set(aPos); // 1 to indicate char
					aPos++;
					// insert huffman code for the char
					String s = gHuffmanCircularDeltas[((int) (aOriginal[i]) + gHuffmanOffset)];
//					System.out.println("#X### "+aOriginal[i]+" "+s);
					for (char c : s.toCharArray()) {
						if (c == '0') {
							bs.set(aPos, false);
						} else if (c == '1') {
							bs.set(aPos);
						}
						aPos++;
					}
					aDelta = 0;
				}
			}
		}
		aPos++; // put 01111 at the end of edit operations
		bs.set(aPos);
		aPos++;
		bs.set(aPos);
		aPos++;
		// System.out.println("edits: " + aSb.toString());
		// return aSb.toString();
		return bs;
	}

	public String calculateDeltaHammingDnaString(String theCandidate, String theOriginal) {
		int aDistance = 0;
		if (theCandidate.equals(theOriginal)) {
			return "";
		}
		StringBuilder aSb = new StringBuilder();
		char[] aCandidate = theCandidate.toCharArray();
		char[] aOriginal = theOriginal.toCharArray();
		if (aCandidate.length <= aOriginal.length) {
			int aDelta = 0;
			for (int i = 0; i < aCandidate.length; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					aDelta++;
				} else {
					if (aDelta == 0) {
						aSb.append(aOriginal[i]);
					} else {
						aSb.append(aDelta + "" + aOriginal[i]);
						aDelta = 0;
					}

				}
			}
		}
		// System.out.println("edits: " + aSb.toString());
		return aSb.toString();
	}

	public double calculateHammingDistance(String theCandidate, String theOriginal) {
		double aDistance = 0;
		char[] aCandidate = theCandidate.toCharArray();
		char[] aOriginal = theOriginal.toCharArray();
		if (aCandidate.length <= aOriginal.length) {
			for (int i = 0; i < aCandidate.length; i++) {
				if (aCandidate[i] != aOriginal[i]) {
					aDistance++;
				}
			}
		}
		return aDistance;
	}

	public BitSet calculateHammingCDBitSet(String theCandidate, String theOriginal) {
		BitSet bs = new BitSet();
		int aPos = 0;
		char[] aCandidate = theCandidate.toCharArray();
		char[] aOriginal = theOriginal.toCharArray();
		if (aCandidate.length <= aOriginal.length) {
			for (int i = 0; i < aCandidate.length; i++) {
				if (aCandidate[i] == aOriginal[i]) {
					bs.set(aPos, false);
					aPos++;
				} else {
					bs.set(aPos);
					aPos++;
					String s = gHuffmanCircularDeltas[((int) (aOriginal[i]) + gHuffmanOffset)];
//					System.out.println("#X### "+aOriginal[i]+" "+s);
					for (char c : s.toCharArray()) {
						if (c == '0') {
							bs.set(aPos, false);
						} else if (c == '1') {
							bs.set(aPos);
						}
						aPos++;
					}
				}
			}
		}
		bs.set(aPos);
		return bs;
	}
}
