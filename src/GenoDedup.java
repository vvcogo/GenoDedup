package genompress;

import data.DataContainer;
import workflows.Workflow;

public class GenoDedup extends Workflow {
	DataContainer gDC;

	public GenoDedup(String theFilename, String theConfigFilename) {
		this.gDC = new DataContainer(theFilename, theConfigFilename);
		prepare();
		run();
		terminate();
	}

	@Override
	public void prepare() {
		for (Workflow aReader : gDC.getReaders()) {
			aReader.prepare();
		}
		for (Workflow aProcessor : gDC.getProcessors()) {
			aProcessor.prepare();
		}
		for (Workflow aWriter : gDC.getWriters()) {
			aWriter.prepare();
		}
	}

	@Override
	public void run() {
		for (Workflow aReader : gDC.getReaders()) {
			aReader.start();
		}
		for (Workflow aProcessor : gDC.getProcessors()) {
			aProcessor.start();
		}
		for (Workflow aWriter : gDC.getWriters()) {
			aWriter.start();
		}
	}

	@Override
	public void terminate() {
		try {
			for (Workflow aReader : gDC.getReaders()) {
				aReader.terminate();
			}
			for (Workflow aReader : gDC.getReaders()) {
				aReader.join();
			}
			for (Workflow aProcessor : gDC.getProcessors()) {
				aProcessor.terminate();
			}
			for (Workflow aProcessor : gDC.getProcessors()) {
				aProcessor.join();
			}
			for (Workflow aWriter : gDC.getWriters()) {
				aWriter.terminate();
			}
			for (Workflow aWriter : gDC.getWriters()) {
				aWriter.join();
			}
			gDC.terminate();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("java Genompress <original_file> <conf_file>");
			System.exit(1);
		}
		new GenoDedup(args[0], args[1]);
	}
}
