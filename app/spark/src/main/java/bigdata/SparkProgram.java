package bigdata;
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
		String inputPath = "/user/lleduc/hgt/";
		//String inputPath = "/user/raw_data/dem3";
		//String inputPath = "/user/philippot/test";
		
		// import files into a PairRDD <filename, stream>
		JavaPairRDD<String, PortableDataStream> importedRDD = context.binaryFiles(inputPath);
		
		// now we convert the bytes stream into shorts
		JavaPairRDD<Tuple2<Integer, Integer>, short[]> processedRDD = importedRDD.mapToPair(BaseFunction.mapBytesToShorts);
				
		// map-reduce to create zoom 0
		JavaPairRDD<String, byte[]> subImagesRDD = processedRDD.flatMapToPair(BaseFunction.to256SizedTiles);
		JavaPairRDD<String, byte[]> subImagesCombinedRDD = subImagesRDD.reduceByKey(BaseFunction.combineImagesWithSameKey);
		
		// map-reduce to create zoom...
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
		
		HBase.setUp();
		
		// create the tiles
		if (args.length != 0){
			switch (args[0]) {
		        case "create-hbase":  {
	        		HBase.createTable();
		            break;
	        	}
		        case "0":  {
			        	addZoomHbase(subImagesCombinedRDD, HBase.ZOOM_0_FAMILY);
			            break;
		        	}
		        case "1":  {
		        		addZoomHbase(zoom1ResizedRDD, HBase.ZOOM_1_FAMILY);
			            break;
		        	}
		        case "2":  {
	        		addZoomHbase(zoom2ResizedRDD, HBase.ZOOM_2_FAMILY);
		            break;
	        	}
		        case "3":  {
	        		addZoomHbase(zoom3ResizedRDD, HBase.ZOOM_3_FAMILY);
		            break;
	        	}
		        case "4":  {
	        		addZoomHbase(zoom4ResizedRDD, HBase.ZOOM_4_FAMILY);
		            break;
	        	}
		        default: {
			        	System.out.println("Unknown command");
			            break;
			        }
				}
		}		
		context.close();	
	}
	
	/*
	private static void createZoomPNG(JavaPairRDD<String, byte[]> rdd){
		rdd.foreach(file -> {
			ImageIO.write(Utils.byteStreamToBufferedImage(file._2), "png", new File(file._1 + ".png"));
		});
	}
	*/
	
	private static void addZoomHbase(JavaPairRDD<String, byte[]> rdd, byte[] zoomLevel){
		rdd.foreach(file -> {
			HBase.addRow(file._1, file._2, zoomLevel);
		});
	}
}