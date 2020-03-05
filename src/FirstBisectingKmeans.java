package pt.ul.fc.di.lasige.SparkKMeans;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class FirstBisectingKmeans {

	private String gInput;
	private int gK;
	private int gNumInteractions;

	private class MyFunc implements Function<String, Vector>, Serializable {

		Vector v;

		public Vector call(String s) throws Exception {
			String[] sarray = s.split(",");
			double[] values = new double[sarray.length];
			for (int i = 0; i < sarray.length; i++) {
				values[i] = Double.parseDouble(sarray[i]);
			}
			v = Vectors.dense(values);
			return v;
		}

		private void writeObject(java.io.ObjectOutputStream out) {
			try {
				out.writeObject(v);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void readObject(java.io.ObjectInputStream in) {
			try {
				v = (Vector) in.readObject();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public FirstBisectingKmeans(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: FirstBisectingKmeans <file> <k> <num_interactions>");
			System.exit(1);
		}
		this.gInput = args[0];
		this.gK = Integer.parseInt(args[1]);
		this.gNumInteractions = Integer.parseInt(args[2]);
		System.out.println("FirstBisectingKmeans: creating spark context");
		SparkConf conf = new SparkConf().setAppName("JavaKMeansExample");
		SparkContext jsc = new SparkContext(conf);
		System.out.println("FirstBisectingKmeans: reading data to JavaRDD");
		JavaRDD<String> data = jsc.textFile(gInput, 1).toJavaRDD();
		Function mapFunc = new MyFunc();
		System.out.println("FirstBisectingKmeans: parsing data");
		JavaRDD<Vector> parsedData = data.map(mapFunc);

		// Cluster the data into two classes using KMeans
		int numClusters = gK;
		int numIterations = gNumInteractions;
		System.out.println("FirstBisectingKmeans: starting the BisectingKMeans");
		BisectingKMeans bkm = new BisectingKMeans().setK(gK).setMaxIterations(gNumInteractions);
		System.out.println("FirstBisectingKmeans: running the BisectingKMeans model");
		BisectingKMeansModel model = bkm.run(parsedData.rdd());
		try {
			System.out.println("FirstBisectingKmeans: writing centroids");
			BufferedWriter aWriter = new BufferedWriter(new FileWriter("./BisectingKmeansCentroids_" + gK + ".txt"));
			for (Vector center : model.clusterCenters()) {
				StringBuilder aSb = new StringBuilder();
				for (double d : center.toArray()) {
					aSb.append(Math.round(d) + ",");
				}
				aSb.delete(aSb.length() - 1, aSb.length());
				aWriter.write(aSb.toString() + "\n");
			}
			System.out.println("FirstBisectingKmeans: finishing the job");
			aWriter.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		new FirstBisectingKmeans(args);
	}

}
