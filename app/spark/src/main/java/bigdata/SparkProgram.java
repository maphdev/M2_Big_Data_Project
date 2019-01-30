package bigdata;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

public class SparkProgram {

	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setAppName("Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		// input path
		//String inputPath = "/user/lleduc/hgt/";
		String inputPath = "/user/raw_data/dem3";
		//String inputPath = "/user/philippot/him";

		// import files into a PairRDD <filename, stream>
		JavaPairRDD<String, PortableDataStream> importedRDD = context.binaryFiles(inputPath);

		// now we convert the bytes stream into shorts
		JavaPairRDD<Tuple2<Integer, Integer>, short[]> processedRDD = importedRDD.mapToPair(BaseFunction.mapBytesToShorts);

		// initial images
		JavaPairRDD<String, byte[]> subImagesRDD = processedRDD.flatMapToPair(BaseFunction.to256SizedTiles);
		JavaPairRDD<String, byte[]> subImagesCombinedRDD = subImagesRDD.reduceByKey(BaseFunction.combineImagesWithSameKey);

		// create hbase
		ToolRunner.run(HBaseConfiguration.create(), new HBase(), args);
		
		// zooms
		processAndHBase(subImagesCombinedRDD);

		context.close();
	}

	//Create png from rdd -> only in order to test small datasets
	private static void createZoomPNG(JavaPairRDD<String, byte[]> rdd){
		rdd.foreach(file -> {
			ImageIO.write(Utils.byteStreamToBufferedImage(file._2), "png", new File(file._1 + ".png"));
		});
	}

	private static void processAndHBase(JavaPairRDD<String, byte[]> RDD0) throws IOException{
		JavaPairRDD<String, byte[]> previousRDD = RDD0;
		
		previousRDD.cache();
		
		HBase.addZoomHbase(previousRDD, 0);

		for (int i = 1; i < 12; i++){
			JavaPairRDD<String, ZoomTile> zoomMapRDD = previousRDD.mapToPair(ZoomFunction.zoomMap);
			JavaPairRDD<String, ZoomTile> zoomReducedRDD = zoomMapRDD.reduceByKey(ZoomFunction.zoomReducer);
			JavaPairRDD<String, byte[]> zoomResizedRDD = zoomReducedRDD.mapToPair(ZoomFunction.zoomResize);
			zoomResizedRDD.cache();
			int zoom = i;
			HBase.addZoomHbase(zoomResizedRDD, zoom);
			previousRDD.unpersist();
			previousRDD = zoomResizedRDD;
		}
	}
}
