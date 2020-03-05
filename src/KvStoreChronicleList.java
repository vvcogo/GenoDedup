package structures.kvstore;

import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.map.*;
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import utils.BitInputStream;
import utils.BitOutputStream;
import utils.Configuration;
import utils.IntValueList;
import utils.IntValueListMarshaller;
import utils.Timer;
import structures.KvStore;

public class KvStoreChronicleList implements KvStore {
	private boolean SERIALIZE_ON_DESTROY = true;
	ChronicleMap<IntValue, IntValueList> gHashTable;
	int gId;

	public KvStoreChronicleList(int theId, Configuration theConf) {
		// try {
		gId = theId;
		if (theConf != null) {
			IntValueList aList = new IntValueList();
			IntValue aValue = Values.newHeapInstance(IntValue.class);
			aValue.setValue(10234567);
			int aNumNeighborsPerKey = (int) ((int) (theConf.getLong("max_entries_kvs", 1_000_000L) * 2)
					/ theConf.getLong("max_entries_lsh", 1_000_000L));
			// aNumNeighborsPerKey = 50; //TODO remove this later
			for (int i = 0; i < (aNumNeighborsPerKey); i++) {
				aList.add(aValue);
			}
			// gHashTable = ChronicleMap.of(IntValue.class, IntValueList.class)
			// .valueMarshaller(IntValueListMarshaller.INSTANCE).averageValue(aList)
			// .entries(theConf.getLong("max_entries_lsh", 1_000_000L))
			// .putReturnsNull(true)
			// .removeReturnsNull(true)
			// // .checksumEntries(false)
			// .create();
			gHashTable = ChronicleMap.of(IntValue.class, IntValueList.class)
					.valueMarshaller(IntValueListMarshaller.INSTANCE).averageValue(aList).entries(100_000_000L)
					.putReturnsNull(true).removeReturnsNull(true)
					// .checksumEntries(false)
					.create();
			// .createOrRecoverPersistedTo(new
			// File("ssd2/LshIndex_"+gId+".dat"));
			File aFile = new File("ssd2/LshIndex_" + gId + ".dat");
			if (aFile.exists() && !aFile.isDirectory()) {
				recoverFromFile("ssd2/LshIndex_" + gId + ".dat");
			}
			System.out.println("Current kvs index size: " + gHashTable.size());
			System.out.println("Max kvs size: " + theConf.getLong("max_entries_lsh", 1_000_000L));
		} else {
			gHashTable = ChronicleMap.of(IntValue.class, IntValueList.class).averageValue(new IntValueList())
					.valueMarshaller(IntValueListMarshaller.INSTANCE).entries(100_000L).create();
			// .createPersistedTo(new File("LshIndex.dat"));
		}
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
	}

	private void recoverFromFile(String theFilename) {
		try {
			BitInputStream aIn = new BitInputStream(theFilename, true);
			while (!aIn.hasFinished()) {
				IntValue aKey = Values.newHeapInstance(IntValue.class);
				aKey.setValue(aIn.readInt());
				int aLen = aIn.readInt();
				// System.out.println(gId+" "+aKey.getValue()+" "+aLen);
				IntValueList aList = new IntValueList();
				for (int i = 0; i < aLen; i++) {
					IntValue aElement = Values.newHeapInstance(IntValue.class);
					aElement.setValue(aIn.readInt());
					aList.add(aElement);
				}
				gHashTable.put(aKey, aList);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public long size() {
		return gHashTable.size();
	}

	public void sadd(String theKey, String theMember) {
		IntValue key = Values.newHeapInstance(IntValue.class);
		key.setValue(Integer.parseInt((String) theKey));
		if (!gHashTable.containsKey(key)) {
			// System.out.println("creating new entry "+key);
			gHashTable.put(key, new IntValueList());
		}
		// System.out.println("get here: "+gHashTable.get(key));
		IntValue member = Values.newHeapInstance(IntValue.class);
		member.setValue(Integer.parseInt((String) theMember));
		IntValueList aList = gHashTable.get(key);
		if (aList == null) {
			aList = new IntValueList();
		}
		aList.add(member);
		// System.out.println(gHashTable.put(key, aNewList));
		// System.out.println(gHashTable.get(key));
		// try {
		// System.out.println(key+" "+aList);
		try {
			gHashTable.put(key, aList);
		} catch (IllegalArgumentException e) {
			System.out.println("IllegalArgumentException: Value too large. Skip adding it.");
			return;
		}
		// } catch (IllegalStateException e) {
		// System.out.println("KvStoreChronicleList: IllegalStateException");
		// throw e;
		// } catch (Exception e){
		// System.out.println("KvStoreChronicleList: Exception");
		// throw e;
		// }
		// System.out.println("get here: "+gHashTable.get(key));
	}

	public void sadd(int theKey, int theMember) {
		IntValue key = Values.newHeapInstance(IntValue.class);
		key.setValue(theKey);
		if (!gHashTable.containsKey(key)) {
			// System.out.println("creating new entry "+key);
			gHashTable.put(key, new IntValueList());
		}
		// System.out.println("get here: "+gHashTable.get(key));
		IntValueList aList = gHashTable.get(key);
		if (aList == null) {
			aList = new IntValueList();
		}
		if (aList.size() < 50) {
			IntValue member = Values.newHeapInstance(IntValue.class);
			member.setValue(theMember);
			aList.add(member);
			// System.out.println(gHashTable.put(key, aNewList));
			// System.out.println(gHashTable.get(key));
			// try {
			try {
				gHashTable.put(key, aList);
			} catch (IllegalArgumentException e) {
				System.out.println("IllegalArgumentException: Value too large. Skip adding it.");
				return;
			}
		}
		// } catch (IllegalStateException e) {
		// System.out.println("KvStoreChronicleList: IllegalStateException");
		// throw e;
		// } catch (Exception e){
		// System.out.println("KvStoreChronicleList: Exception");
		// throw e;
		// }
		// System.out.println("get here: "+gHashTable.get(key));
	}

	public Set<String> smembers(String theKey) {
		IntValue key = Values.newHeapInstance(IntValue.class);
		key.setValue(Integer.parseInt((String) theKey));
		// System.out.println("query key: "+key);
		Set<String> aSet = new HashSet<String>();
		if (theKey != null) {
			IntValueList aList = gHashTable.get(key);
			// System.out.println("Returned kv: "+aList);
			if (aList != null) {
				for (int i = 0; i < aList.size(); i++) {
					aSet.add(Integer.toString(aList.get(i).getValue()));
				}
			}
		}
		// System.out.println("#size "+gId+" key: "+theKey+" size:
		// "+aSet.size());;
		return aSet;
	}

	@Override
	public void set(String theKey, String theValue) {
		try {
			sadd(theKey, theValue);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void set(int theDb, String theKey, String theValue) {
		try {
			sadd(theKey, theValue);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean contains(String theKey) {
		IntValue key = Values.newHeapInstance(IntValue.class);
		key.setValue(Integer.parseInt((String) theKey));
		return gHashTable.containsKey(key);
	}

	@Override
	public boolean contains(int theDb, String theKey) {
		IntValue key = Values.newHeapInstance(IntValue.class);
		key.setValue(Integer.parseInt((String) theKey));
		return gHashTable.containsKey(key);
	}

	@Override
	public String get(String theKey) {
		Set<String> aQuery = smembers(theKey);
		String[] aTemplate = new String[aQuery.size()];
		if (aQuery.size() > 0) {
			return aQuery.toArray(aTemplate)[0];
		} else {
			return null;
		}
	}

	@Override
	public String get(int theDb, String theKey) {
		return get(theKey);
	}

	@Override
	public void destroy() {
		if (SERIALIZE_ON_DESTROY) {
			File aFile = new File("ssd2/LshIndex_" + gId + ".dat");
			if (!aFile.exists()) {
				BitOutputStream aOut = new BitOutputStream("ssd2/LshIndex_" + gId + ".dat", true);
				System.out.println(gId + " " + gHashTable.keySet().size());
				for (IntValue i : gHashTable.keySet()) {
					aOut.writeInt(i.getValue());
					IntValueList aList = gHashTable.get(i);
					if (aList != null) {
						aOut.writeInt(aList.size());
						// System.out.println(gId+" "+i.getValue()+" "+"
						// "+aList.size());
						for (IntValue j : aList) {
							if (j != null) {
								aOut.writeInt(j.getValue());
								// System.out.println(gId+" "+i+" "+" "+j);
							} else {
								System.out.println("[WRN] Found null IntValue");
							}
						}
					}
				}
				aOut.flush();
				aOut.close();
			}
		}
		gHashTable.close();
		return;
	}

	@Override
	public List<String> scan() {
		// TODO
		return null;
	}

	@Override
	public Set<String> sunion(List<String> theKeys) {
		Set<String> aResult = new HashSet<String>();
		for (String s : theKeys) {
			aResult.addAll((Collection<? extends String>) smembers(s));
		}
		return aResult;
	}

	@Override
	public List<String> get(List<String> theKeys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove(String theChunk, String theKey) {
		// int[] aOldArray = gHashTable.get(Integer.parseInt(theKey));
		// if (aOldArray != null && aOldArray.length > 0) {
		// int aMember = Integer.parseInt(theMember);
		// int aFoundTimes = 0;
		// for (int i = 0; i < aOldArray.length; i++) {
		// if (aOldArray[i] == aMember) {
		// aFoundTimes++;
		// }
		// }
		// if (aFoundTimes > 0) {
		// if ((aOldArray.length - aFoundTimes > 0)) {
		// int[] aNewArray = new int[aOldArray.length - aFoundTimes];
		// int i = 0, j = 0;
		// for (; i < aOldArray.length;) {
		// if (aOldArray[i] != aMember) {
		// aNewArray[j] = aOldArray[i];
		// j++;
		// }
		// i++;
		// }
		// gHashTable.put(Integer.parseInt(theKey), aNewArray);
		// } else {
		// gHashTable.remove(Integer.parseInt(theKey));
		// }
		// }
		// }

	}
}
