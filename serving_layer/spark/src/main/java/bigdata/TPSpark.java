package bigdata;
import java.io.File;
import javax.imageio.ImageIO;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
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
		JavaPairRDD<String, ZoomTile> zoom1MapRDD = subImagesCombinedRDD.mapToPair(ZoomFunction.zoomMap);
		JavaPairRDD<String, ZoomTile> zoom1ReducedRDD = zoom1MapRDD.reduceByKey(ZoomFunction.zoomReducer);
		JavaPairRDD<String, byte[]> zoom1ResizedRDD = zoom1ReducedRDD.mapToPair(ZoomFunction.zoomResize);
		
		JavaPairRDD<String, ZoomTile> zoom2MapRDD = zoom1ResizedRDD.mapToPair(ZoomFunction.zoomMap);
		JavaPairRDD<String, ZoomTile> zoom2ReducedRDD = zoom2MapRDD.reduceByKey(ZoomFunction.zoomReducer);
		JavaPairRDD<String, byte[]> zoom2ResizedRDD = zoom2ReducedRDD.mapToPair(ZoomFunction.zoomResize);
		
		JavaPairRDD<String, ZoomTile> zoom3MapRDD = zoom2ResizedRDD.mapToPair(ZoomFunction.zoomMap);
		JavaPairRDD<String, ZoomTile> zoom3ReducedRDD = zoom3MapRDD.reduceByKey(ZoomFunction.zoomReducer);
		JavaPairRDD<String, byte[]> zoom3ResizedRDD = zoom3ReducedRDD.mapToPair(ZoomFunction.zoomResize);
		
		JavaPairRDD<String, ZoomTile> zoom4MapRDD = zoom3ResizedRDD.mapToPair(ZoomFunction.zoomMap);
		JavaPairRDD<String, ZoomTile> zoom4ReducedRDD = zoom4MapRDD.reduceByKey(ZoomFunction.zoomReducer);
		JavaPairRDD<String, byte[]> zoom4ResizedRDD = zoom4ReducedRDD.mapToPair(ZoomFunction.zoomResize);
		
		// create the tiles
		if (args.length != 0){
			switch (args[0]) {
		        case "0":  {
			        	System.out.println("0");
			        	createZoom(subImagesCombinedRDD);
			            break;
		        	}
		        case "1":  {
		        		System.out.println("1");
		        		createZoom(zoom1ResizedRDD);
			            break;
		        	}
		        case "2":  {
	        		System.out.println("2");
	        		createZoom(zoom2ResizedRDD);
		            break;
	        	}
		        case "3":  {
	        		System.out.println("3");
	        		createZoom(zoom3ResizedRDD);
		            break;
	        	}
		        case "4":  {
	        		System.out.println("4");
	        		createZoom(zoom4ResizedRDD);
		            break;
	        	}
		        default: {
			        	System.out.println("Please specify a number corresponding to a level of zoom");
			            break;
			        }
				}
		}		
		context.close();	
	}
	
	private static void createZoom(JavaPairRDD<String, byte[]> rdd){
		rdd.foreach(file -> {
			ImageIO.write(Utils.byteStreamToBufferedImage(file._2), "png", new File(file._1 + ".png"));
		});
	}
}