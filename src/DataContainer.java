package data;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import channels.CommunicationChannel;
import factories.*;
import structures.KvStore;
import structures.LshIndex;
import utils.Configuration;
import utils.Timer;
import workflows.Workflow;
import workflows.monitor.Monitor;

public class DataContainer {
	private String gInputFilename, gOutputFilename, gBasename, gExtension;
	private int gNumGroups, gNumProcessorsPerGroup;
	private Configuration gConf;
	private ArrayList<Workflow> gReaders;
	private ArrayList<Workflow> gProcessors;
	private ArrayList<Workflow> gWriters;
//	private LshIndex gLshDna, gLshQuality;
//	private KvStore gKvsDna, gKvsQuality, gKvsOperations;
	private LshIndex gLshDna;
	private KvStore gKvsDna;
	private ArrayList<MeasureContainer> gMeasureContainers;
	private ArrayList<CommunicationChannel> gChannelsInput;
	private ArrayList<CommunicationChannel> gChannelsOutput;
	private ScheduledExecutorService gExecutor;
	private String gHeaderPattern;

	public DataContainer(String theOriginalFilename, String theConfigFilename) {
		this.gInputFilename = theOriginalFilename;
		this.gConf = new Configuration(theConfigFilename);
		try {
			prepare();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (UnsupportedOperationException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void prepare() throws IllegalArgumentException, UnsupportedOperationException {
		// System.out.println(gOriginalFilename);
		String[] aFilenameSplit = gInputFilename.split("\\.");
		// for(String s : aFilenameSplit){
		// System.out.println(s);
		// }
		this.gExtension = aFilenameSplit[aFilenameSplit.length - 1];
		if (!isValidExtension(gExtension)) {
			throw new IllegalArgumentException("Invalid file extension " + gExtension);
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < aFilenameSplit.length - 1; i++) {
			sb.append(aFilenameSplit[i]);
			if (i < aFilenameSplit.length - 2) {
				sb.append(".");
			}
		}
		this.gBasename = sb.toString();
		this.gOutputFilename = gBasename + "." + getOutputExtension(gExtension);
		// System.out.println(gBasename);
		String aExecutionType = getExecutionType(gExtension);
		this.gNumGroups = gConf.getInt(aExecutionType + ".groups", 1);
		this.gNumProcessorsPerGroup = gConf.getInt(aExecutionType + ".processors_per_group");
		// System.out.println(gNumGroups);
		this.gHeaderPattern = gConf.get("header_pattern", "");
		// System.out.println(gHeaderPattern);
		createKvStores();
		createLshIndexes();
		createGroups();
		createMonitors();
	}

	public void terminate() {
		this.gExecutor.shutdownNow();
		this.gKvsDna.destroy();
//		this.gKvsQuality.destroy();
//		this.gKvsOperations.destroy();
		this.gLshDna.destroy();
//		this.gLshQuality.destroy();
	}

	private boolean isValidExtension(String theExtension) {
		ArrayList<String> aListExtensions = new ArrayList<String>(
				Arrays.asList("fastq", "fq", "sam", "bam", "fasta", "fa", "fqz", "samz", "bamz", "faz"));
		return aListExtensions.contains(theExtension);
	}

	private String getExecutionType(String theExtension) {
		switch (theExtension) {
		case "fastq":
		case "fq":
		case "sam":
		case "bam":
		case "fasta":
		case "fa":
			return "compression";
		case "fqz":
		case "samz":
		case "bamz":
		case "faz":
			return "decompression";
		default:
			return null;
		}
	}

	private String getOutputExtension(String theExtension) {
		switch (theExtension) {
		case "fastq":
		case "fq":
			return "fqz";
		case "sam":
			return "samz";
		case "bam":
			return "bamz";
		case "fasta":
		case "fa":
			return "faz";
		case "fqz":
			return "fastq";
		case "samz":
			return "sam";
		case "bamz":
			return "bam";
		case "faz":
			return "fasta";
		default:
			return null;
		}
	}

	private void createKvStores() {
		long t1 = Timer.ns();
		this.gKvsDna = KvStoreFactory.getKvStore(DataType.DNA, gConf);
		long t2 = Timer.ns();
		System.out.println("Time to load kvs_dna: "+(t2-t1)+" ns, size: "+gKvsDna.size());
//		this.gKvsQuality = KvStoreFactory.getKvStore(DataType.QUALITY, gConf);
//		t1 = Timer.ns();
//		System.out.println("Time to load kvs_quality: "+(t1-t2)+" ns, size: "+gKvsQuality.size());
//		this.gKvsOperations = KvStoreFactory.getKvStore(DataType.OPERATIONS, gConf);
//		t2 = Timer.ns();
//		System.out.println("Time to load kvs_operations: "+(t2-t1)+" ns, size: "+gKvsOperations.size());
	}

	private void createLshIndexes() {
		long t1 = Timer.ns();
		this.gLshDna = LshIndexFactory.getLshIndex(DataType.DNA, gConf);
		long t2 = Timer.ns();
		System.out.println("Time to load lsh_dna: "+(t2-t1)+" ns");
//		this.gLshQuality = LshIndexFactory.getLshIndex(DataType.QUALITY, gConf);
//		t1 = Timer.ns();
//		System.out.println("Time to load lsh_quality: "+(t1-t2)+" ns");
	}

	private void createGroups() {
		long aSizeBytes = new File(gInputFilename).length();
		long aBytesPerReader = aSizeBytes / gNumGroups;
		this.gMeasureContainers = new ArrayList<MeasureContainer>(gNumGroups);
		this.gReaders = new ArrayList<Workflow>(gNumGroups);
		this.gProcessors = new ArrayList<Workflow>(gNumProcessorsPerGroup);
		this.gWriters = new ArrayList<Workflow>(gNumGroups);
		this.gChannelsInput = new ArrayList<CommunicationChannel>();
		this.gChannelsOutput = new ArrayList<CommunicationChannel>();
		for (int i = 0; i < gNumGroups; i++) {
			this.gMeasureContainers.add(new MeasureContainer());
			this.gChannelsInput
					.add(CommunicationChannelFactory.getCommunicationChannel(CommunicationStep.INPUT, gConf));
			this.gChannelsOutput
					.add(CommunicationChannelFactory.getCommunicationChannel(CommunicationStep.OUTPUT, gConf));
			if (i < gNumGroups - 1) {
				this.gReaders
						.add(WorkflowFactory.getReader(this, i, i * aBytesPerReader, (i + 1) * aBytesPerReader - 1));
			} else {
				this.gReaders.add(WorkflowFactory.getReader(this, i, i * aBytesPerReader, aSizeBytes + 1));
			}
			for (int j = 0; j < gNumProcessorsPerGroup; j++) {
				this.gProcessors.add(WorkflowFactory.getProcessor(this, i));
			}
			this.gWriters.add(WorkflowFactory.getWriter(this, i));
		}
	}

	private void createMonitors() {
		Runnable aMonitor = new Monitor(this);
		this.gExecutor = Executors.newScheduledThreadPool(1);
		this.gExecutor.scheduleAtFixedRate(aMonitor, 0, 1, TimeUnit.SECONDS);
	}

	public ArrayList<Workflow> getReaders() {
		return gReaders;
	}

	public ArrayList<Workflow> getProcessors() {
		return gProcessors;
	}

	public ArrayList<Workflow> getWriters() {
		return gWriters;
	}

	public Configuration getConf() {
		return gConf;
	}

	public String getBasename() {
		return gBasename;
	}

	public String getExtension() {
		return gExtension;
	}

	public String getInputFilename() {
		return gInputFilename;
	}

	public String getOutputFilename() {
		return gOutputFilename;
	}

	public String getHeaderPattern() {
		return gHeaderPattern;
	}

	public void setHeaderPattern(String theHeaderPattern) {
		this.gHeaderPattern = theHeaderPattern;
	}

	public LshIndex getLshDna() {
		return gLshDna;
	}

	public LshIndex getLshQuality() {
//		return gLshQuality;
		return null;
	}

	public KvStore getKvsDna() {
		return gKvsDna;
	}

	public KvStore getKvsQuality() {
//		return gKvsQuality;
		return null;
	}
	
	public KvStore getKvsOperations(){
		return null;
//		return gKvsOperations;
	}

	public CommunicationChannel getChannelInput(int theGroup) {
		if (theGroup < gChannelsInput.size()) {
			return gChannelsInput.get(theGroup);
		} else {
			return null;
		}
	}

	public CommunicationChannel getChannelOutput(int theGroup) {
		if (theGroup < gChannelsOutput.size()) {
			return gChannelsOutput.get(theGroup);
		} else {
			return null;
		}
	}

	public MeasureContainer getMeasureContainer(int theGroup) {
		if (theGroup < gMeasureContainers.size()) {
			return gMeasureContainers.get(theGroup);
		} else {
			return null;
		}
	}

	public int getNumGroups() {
		return gNumGroups;
	}

	public ArrayList<MeasureContainer> getMeasureContainers() {
		return gMeasureContainers;
	}
}
