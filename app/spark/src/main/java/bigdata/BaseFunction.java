package bigdata;

import java.awt.image.BufferedImage;
import java.awt.Graphics;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.input.PortableDataStream;

import scala.Tuple2;

public class BaseFunction {
	
	// convert PortableDataStream to short[]
	public static PairFunction<Tuple2<String, PortableDataStream>, Tuple2<Integer, Integer>, short[]> mapBytesToShorts = new PairFunction<Tuple2<String, PortableDataStream>, Tuple2<Integer, Integer>, short[]>() {
		
		private static final long serialVersionUID = -6350258633341747009L;

		public Tuple2<Tuple2<Integer, Integer>, short[]> call(Tuple2<String, PortableDataStream> file) throws Exception {
			Tuple2<Integer, Integer> tileCoords = Utils.getTileCoordsByFileName(file._1);
			short[] content = Utils.convertByteArrayToShortArray(file._2.toArray());
			
			return new Tuple2<Tuple2<Integer, Integer>, short[]>(tileCoords, content);
		}

	};
	
	// get RDD with all 256*256 subImages from tiles, in byte[]
	public static PairFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, short[]>, String, byte[]> to256SizedTiles = new PairFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, short[]>, String, byte[]>() {

		private static final long serialVersionUID = 6295049499770038007L;
		public static final int minTgtPerTile = 5;
		public static final int minSizeTmpImage = 1280;
		public static final int maxTgtPerTile = 6;
		public static final int maxSizeTmpImage = 1536;

		@Override
		public Iterator<Tuple2<String, byte[]>> call(Tuple2<Tuple2<Integer, Integer>, short[]> file)
				throws Exception {
			
			List<Tuple2<String, byte[]>> res = new ArrayList<Tuple2<String, byte[]>>();
			
			// all the coordinates useful for later
			int yTile = file._1._1;
			int xTile = file._1._2;
			
			int yOriginInBigImage = yTile * Utils.tileSize;
			int xOriginInBigImage = xTile * Utils.tileSize;
			
			int y_min = yOriginInBigImage % Utils.smallTileSize;
			int x_min = xOriginInBigImage % Utils.smallTileSize;
			
			int y_max = Utils.tileSize + y_min;
			int x_max = Utils.tileSize + x_min;
			
			Tuple2<Integer, Integer> tmpImgY = y_max <= (5 * Utils.smallTileSize)? new Tuple2<Integer, Integer>(minTgtPerTile, minSizeTmpImage): new Tuple2<Integer, Integer>(maxTgtPerTile, maxSizeTmpImage);
			Tuple2<Integer, Integer> tmpImgX = x_max <= (5 * Utils.smallTileSize)? new Tuple2<Integer, Integer>(minTgtPerTile, minSizeTmpImage): new Tuple2<Integer, Integer>(maxTgtPerTile, maxSizeTmpImage);

			// here we start the magical work
			BufferedImage image = new BufferedImage(tmpImgX._2, tmpImgY._2, BufferedImage.TYPE_INT_ARGB);
			
			short[] heights = file._2;
			int a = 0;
			
			for (int j = y_min; j < y_max; j++) {
				for (int i = x_min; i < x_max; i++) {
					image.setRGB(i, j, Utils.getColor(heights[a]));
					a++;
				}
			}
			
			for (int j = 0; j < tmpImgY._1; j++){
				for (int i = 0; i < tmpImgX._1; i++) {
					int indexY = yOriginInBigImage / Utils.smallTileSize + j;
					int indexX = xOriginInBigImage / Utils.smallTileSize + i;
					BufferedImage subImage = image.getSubimage(i * Utils.smallTileSize, j * Utils.smallTileSize, Utils.smallTileSize, Utils.smallTileSize);
					byte[] subImageInBytes = Utils.bufferedImageToByteStream(subImage);
					res.add(new Tuple2<String, byte[]>(indexY + "-" + indexX, subImageInBytes));
				}
			}
			
			return res.iterator();
		}
		
	};
		
	// we combine all the images with the same key
	public static Function2<byte[], byte[], byte[]> combineImagesWithSameKey = new Function2<byte[], byte[], byte[]>() {

		private static final long serialVersionUID = 2044592339629795644L;

		@Override
		public byte[] call(byte[] oneBytes, byte[] twoBytes) throws Exception {
			BufferedImage oneImage = Utils.byteStreamToBufferedImage(oneBytes);
			BufferedImage twoImage = Utils.byteStreamToBufferedImage(twoBytes);
			
			
			BufferedImage combinedImage = new BufferedImage(Utils.smallTileSize, Utils.smallTileSize, BufferedImage.TYPE_INT_ARGB);
			
			Graphics g = combinedImage.getGraphics();
			g.drawImage(oneImage, 0, 0, null);
			g.drawImage(twoImage, 0, 0, null);
			
			return Utils.bufferedImageToByteStream(combinedImage);
		}
		
	};
	
	// convert big tile in short[] format to RGB BufferedImage
	public static PairFunction<Tuple2<Tuple2<Integer, Integer>, short[]>, Tuple2<Integer, Integer>, BufferedImage> toBufferedImage = new PairFunction<Tuple2<Tuple2<Integer, Integer>, short[]>, Tuple2<Integer, Integer>, BufferedImage>() {

		private static final long serialVersionUID = -5953046102975386641L;

		@Override
		public Tuple2<Tuple2<Integer, Integer>, BufferedImage> call(Tuple2<Tuple2<Integer, Integer>, short[]> file) throws Exception {
			BufferedImage image = new BufferedImage(Utils.tileSize, Utils.tileSize, BufferedImage.TYPE_INT_ARGB);
			
			short[] heights = file._2;
			int a = 0;
			
			for (int j = 0; j < Utils.tileSize; j++) {
				for (int i = 0; i < Utils.tileSize; i++) {
					image.setRGB(i, j, Utils.getColor(heights[a]));
					a++;
				}
			}

			return new Tuple2<Tuple2<Integer, Integer>, BufferedImage>(file._1, image);
		}
		
	};
}