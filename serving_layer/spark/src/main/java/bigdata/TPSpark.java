package bigdata;
import java.io.File;
import javax.imageio.ImageIO;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

public class TPSpark {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		// input path
		String inputPath = "/user/lleduc/hgt/";
		//String inputPath = "/user/raw_data/dem3";
		//String inputPath = "/user/philippot/test";
		
		// import files into a PairRDD <filename, stream>
		JavaPairRDD<String, PortableDataStream> importedRDD = context.binaryFiles(inputPath);
		
		// now we convert the bytes stream into shorts
		JavaPairRDD<Tuple2<Integer, Integer>, short[]> processedRDD = importedRDD.mapToPair(Utils.mapBytesToShorts);
		
		// and we correct the values that are not correct (negative)
		JavaPairRDD<Tuple2<Integer, Integer>, short[]> correctedRDD = processedRDD.mapToPair(Utils.correctValues);
		
		// from all the short[], we get a new RDD with all the sub-images of size 256*256, in byte[]
		JavaPairRDD<String, byte[]> subImagesRDD = correctedRDD.flatMapToPair(Utils.to256SizedTiles);
		
		// we combine the sub-images with the same key, to get the full sub-images and get rid of the fragments
		JavaPairRDD<String, byte[]> subImagesCombinedRDD = subImagesRDD.reduceByKey(Utils.combineImagesWithSameKey);
		
		/*
		System.out.println(subImagesCombinedRDD.count());
		
		subImagesCombinedRDD.foreach(file -> {
			ImageIO.write(Utils.byteStreamToBufferedImage(file._2), "png", new File(file._1 + ".png"));
		});
		*/
		
		JavaPairRDD<String, ZoomTile> zoomOut1MapRDD = subImagesCombinedRDD.mapToPair(Utils.zoomOut1Map);
		
		JavaPairRDD<String, ZoomTile> zoomOut1ReducedRDD = zoomOut1MapRDD.reduceByKey(Utils.zoomOut1Reducer);

		/*
		zoomOut1ReducedRDD.foreach(file -> {
			ImageIO.write(Utils.byteStreamToBufferedImage(file._2.getImage()), "png", new File(file._1 + ".png"));
		});
		*/
		
		JavaPairRDD<String, byte[]> zoomOut1CorrectedRDD = zoomOut1ReducedRDD.mapToPair(Utils.zoomOut1ReducerCorrection);
		
		/*
		zoomOut1FinishedRDD.foreach(file -> {
			ImageIO.write(Utils.byteStreamToBufferedImage(file._2), "png", new File(file._1 + ".png"));
		});
		*/
		
		JavaPairRDD<String, byte[]> zoomOut1FinishedRDD = zoomOut1CorrectedRDD.mapToPair(Utils.zoomOut1ReducerFinished);
		zoomOut1FinishedRDD.foreach(file -> {
			ImageIO.write(Utils.byteStreamToBufferedImage(file._2), "png", new File(file._1 + ".png"));
		});
		
		
		context.close();	
	}
}