package bigdata;
import java.io.File;
import javax.imageio.ImageIO;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
				
		// map-reduce to create zoom 0
		JavaPairRDD<String, byte[]> subImagesRDD = processedRDD.flatMapToPair(Utils.to256SizedTiles);
		JavaPairRDD<String, byte[]> subImagesCombinedRDD = subImagesRDD.reduceByKey(Utils.combineImagesWithSameKey);

		// map-reduce to create zoom 1
		JavaPairRDD<String, ZoomTile> zoom0MapRDD = subImagesCombinedRDD.mapToPair(Utils.zoom0Map);
		JavaPairRDD<String, ZoomTile> zoom0ReducedRDD = zoom0MapRDD.reduceByKey(Utils.zoom0Reducer);
		JavaPairRDD<String, byte[]> zoom0ResizedRDD = zoom0ReducedRDD.mapToPair(Utils.zoom0Resize);
		
		// create the tiles
		if (args.length != 0){
			switch (args[0]) {
		        case "0":  {
			        	System.out.println("0");
			        	createZoom0(subImagesCombinedRDD);
			            break;
		        	}
		        case "1":  {
		        		System.out.println("1");
		        		createZoom1(zoom0ResizedRDD);
		        		subImagesCombinedRDD.unpersist();
			            break;
		        	}
		        default: {
			        	System.out.println("Please specify a number corresponding to a level of zoom");
			            break;
			        }
				}
		}

		/*
		zoomOut1ReducedRDD.foreach(file -> {
			ImageIO.write(Utils.byteStreamToBufferedImage(file._2.getImage()), "png", new File(file._1 + ".png"));
		});
		*/		
		
		context.close();	
	}
	
	private static void createZoom0(JavaPairRDD<String, byte[]> rdd){
		rdd.foreach(file -> {
			ImageIO.write(Utils.byteStreamToBufferedImage(file._2), "png", new File(file._1 + ".png"));
		});
	}
	
	private static void createZoom1(JavaPairRDD<String, byte[]> rdd){
		rdd.foreach(file -> {
			ImageIO.write(Utils.byteStreamToBufferedImage(file._2), "png", new File(file._1 + ".png"));
		});
	}
}