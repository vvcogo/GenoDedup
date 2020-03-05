package structures.lshindex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import structures.KvStore;
import structures.LshIndex;
import structures.kvstore.KvStoreChronicleList;
import utils.Configuration;
import utils.Tuple;

public class LshIndexBitSampling implements LshIndex {
	private float gThreshold;
	private int gNumPermutations;
	private int gNumHashTables;
	private int gChunkSize;
	private ArrayList<KvStore> gKvStores;
	private ArrayList<Tuple<Integer, Integer>> gHashRanges;
	private int gFirstPort;
	private int gLastPort;
	private int gRadius;

	public LshIndexBitSampling(Configuration theConf, float theThreshold, int theNumPermutations, String theLshType) {
		this.gThreshold = theThreshold;
//		System.out.println("gThreshold: " + gThreshold);
		this.gNumPermutations = theNumPermutations;
//		System.out.println("gNumPermutations: " + gNumPermutations);
		this.gRadius = (int) Math.ceil(gNumPermutations - (gNumPermutations * gThreshold));
//		System.out.println("gRadius: " + gRadius);
		this.gNumHashTables = 0;
		for (int k = 1; k < gRadius; k++) {
			if (Math.floor(gRadius / k) == 1 && Math.floor(gNumPermutations / k) < 32) {
				this.gNumHashTables = k;
				break;
			}
		}
		System.out.println("gNumHashTables: " + gNumHashTables);
		if(gNumHashTables==0){
			gNumHashTables=1;
		}
		this.gChunkSize = (int) Math.floor(gNumPermutations / gNumHashTables);
		System.out.println("gChunkSize: " + gChunkSize);
		this.gKvStores = new ArrayList<KvStore>(gNumHashTables);
		this.gFirstPort = 0;
		this.gLastPort = gFirstPort + gNumHashTables;
		for (int i = gFirstPort; i < gLastPort; i++) {
			gKvStores.add(new KvStoreChronicleList(i, theConf));
		}
		gHashRanges = new ArrayList<Tuple<Integer, Integer>>();
		for (int i = 0; i < this.gNumHashTables; i++) {
			gHashRanges.add(new Tuple<Integer, Integer>(i * this.gChunkSize, (i + 1) * this.gChunkSize));
		}
	}

	public LshIndexBitSampling(Configuration theConf, String theLshType) {
		this(theConf, theConf.getFloat(theLshType+".similarity_threshold", 0.95f), theConf.getInt(theLshType+".num_permutations", 128), theLshType);
	}

	public void insert(ArrayList<Integer> theChunks, Integer theKey) {
//		if(theChunks.size()!=gKvStores.size()){
//			System.out.println("[ERROR] Different sizes for chunks ("+theChunks.size()+") and hashtables ("+gKvStores.size()+")");
//		}
		for (int i = gFirstPort, j = 0; i < gLastPort; i++, j++) {
//			System.out.println("Adding "+theChunks.get(j)+" <- "+theKey+" in hashtable "+j);
//			try{
			gKvStores.get(j).sadd(theChunks.get(j).toString(), Integer.toString(theKey));
//			} catch(Exception e){
//				throw e;
//			}
		}
	}

	public Set<Integer> query(ArrayList<Integer> theChunks, int theMaxCandidates) {
//		if(theChunks.size()!=gKvStores.size()){
//			System.out.println("[ERROR] Different sizes for chunks ("+theChunks.size()+") and hashtables ("+gKvStores.size()+")");
//		}
		int aCandidatesSize = 0;
		Set<Integer> aCandidates = new HashSet<Integer>();
		for (int i = gFirstPort, j = 0; i < gLastPort; i++, j++) {
			ArrayList<Integer> aCloseChunks = createCloseChunks(theChunks.get(j));
			for (Integer aCloseChunk : aCloseChunks) {
//				System.out.println("querying closechunk "+theChunks.get(j)+ " in hashtable "+j);
				Set<String> aHashSet = gKvStores.get(j).smembers(aCloseChunk.toString());
				if (aHashSet != null && !aHashSet.isEmpty()) {
					for (String s : aHashSet) {
//						System.out.println("returned candidate: "+s);
						if (aCandidatesSize < theMaxCandidates) {
							aCandidates.add(Integer.parseInt(s));
							aCandidatesSize++;
						} else {
							return aCandidates;
						}
					}
				}
			}
		}
		return aCandidates;
	}
	
	public void printSize(Integer theChunk){
		for(KvStore k : gKvStores){
			k.smembers(theChunk.toString());
		}
	}

	public Set<Integer> query(Integer theHashTable, Integer theChunk, int theMaxCandidates) {
		int aCandidatesSize = 0;
		Set<Integer> aCandidates = new HashSet<Integer>();
			ArrayList<Integer> aCloseChunks = createCloseChunks(theChunk);
			for (Integer aCloseChunk : aCloseChunks) {
//				System.out.println("querying closechunk "+theChunks.get(j)+ " in hashtable "+j);
				Set<String> aHashSet = gKvStores.get(theHashTable).smembers(aCloseChunk.toString());
				if (aHashSet != null && !aHashSet.isEmpty()) {
					for (String s : aHashSet) {
//						System.out.println("returned candidate: "+s);
						if (aCandidatesSize < theMaxCandidates) {
							aCandidates.add(Integer.parseInt(s));
							aCandidatesSize++;
						} else {
							return aCandidates;
						}
					}
				}
			}
			return aCandidates;
		}
	

	public int getNumDbs() {
		return (gLastPort - gFirstPort);
	}

	public void destroy() {
		for (KvStore k : gKvStores){
			k.destroy();
		}
	}

	public List<String> scan() {
		return null;
	}

	public Set<String> smembers(String theKey) {
		return gKvStores.get(0).smembers(theKey);
	}

	public int getNumHashTables() {
		return gNumHashTables;
	}

	public int getChunkSize() {
		return gChunkSize;
	}
	
	public int getRadius(){
		return gRadius;
	}

	public ArrayList<Integer> createCloseChunks(Integer theOriginalChunk) {
		String aFormat = "%"+getChunkSize()+"s";
		String aOriginalChunk = String.format(aFormat, Integer.toBinaryString(theOriginalChunk)).replace(' ', '0');
//		System.out.println("theOriginalChunk: "+theOriginalChunk + " chunjksize: "+getChunkSize());
		ArrayList<Integer> aResult = new ArrayList<Integer>(aOriginalChunk.length());
		StringBuilder sb = new StringBuilder(gChunkSize);
		sb.append(aOriginalChunk);
		aResult.add(Integer.parseInt(sb.toString(), 2));
		for (int i = 0; i < aOriginalChunk.length(); i++) {
			if (aOriginalChunk.charAt(i) == '1') {
				sb.setCharAt(i, '0');
				aResult.add(Integer.parseInt(sb.toString(), 2));
				sb.setCharAt(i, '1');
			} else {
				sb.setCharAt(i, '1');
				aResult.add(Integer.parseInt(sb.toString(), 2));
				sb.setCharAt(i, '0');
			}
		}
		return aResult;
	}

	@Override
	public void remove(Integer theHashTable, Integer theChunk, Integer theKey) {
		gKvStores.get(theHashTable).remove(Integer.toString(theChunk), Integer.toString(theKey));
	}
}
