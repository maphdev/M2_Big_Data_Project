package bigdata;

import java.awt.image.BufferedImage;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Image;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.imageio.ImageIO;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

public class Utils {
	
	public static final int tileSize = 1201;
	public static final int smallTileSize = 256;
	public static final int halfNumberTilesLong = 90;
	public static final int halfNumberTilesLat = 180;

	// convert PortableDataStream to short[]
	public static PairFunction<Tuple2<String, PortableDataStream>, Tuple2<Integer, Integer>, short[]> mapBytesToShorts = new PairFunction<Tuple2<String, PortableDataStream>, Tuple2<Integer, Integer>, short[]>() {
		
		private static final long serialVersionUID = -6350258633341747009L;

		public Tuple2<Tuple2<Integer, Integer>, short[]> call(Tuple2<String, PortableDataStream> file) throws Exception {
			Tuple2<Integer, Integer> tileCoords = getTileCoords(file._1);
			short[] content = convertByteArrayToShortArray(file._2.toArray());
			
			return new Tuple2<Tuple2<Integer, Integer>, short[]>(tileCoords, content);
		}

	};
	
	private static Tuple2<Integer, Integer> getTileCoords (String path) {
		String fullFileName = path.substring(path.lastIndexOf("/")+1);
		String fileName = fullFileName.substring(0, fullFileName.lastIndexOf("."));
		    
        String longLabel = fileName.substring(0, 1).toLowerCase();
		Integer longValue = Integer.parseInt(fileName.substring(1, 3));
		
		String latLabel = fileName.substring(3, 4).toLowerCase();
		Integer latValue = Integer.parseInt(fileName.substring(4, 7));
		
		if (longLabel.equals("n")) {
			longValue = halfNumberTilesLong - longValue - 1;
		} else if (longLabel.equals("s")) {
			longValue = halfNumberTilesLong + longValue - 1;
		}
		
		if (latLabel.equals("w")) {
			latValue = halfNumberTilesLat - latValue;
		} else if (latLabel.equals("e")) {
			latValue = halfNumberTilesLat + latValue;
		}
		
		return new Tuple2<Integer, Integer>(longValue, latValue);
	}
	
	private static short[] convertByteArrayToShortArray (byte[] bytes) {
		short[] shorts = new short[tileSize * tileSize];
		if (shorts.length == bytes.length/2){
			ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asShortBuffer().get(shorts).rewind();
		}
		return shorts;
	}
	
	// correct values if < 0 or > 255
	public static PairFunction<Tuple2<Tuple2<Integer, Integer>, short[]>, Tuple2<Integer, Integer>, short[]> correctValues = new PairFunction<Tuple2<Tuple2<Integer, Integer>, short[]>, Tuple2<Integer, Integer>, short[]>() {
		
		private static final long serialVersionUID = 1337462003487647478L;

		public Tuple2<Tuple2<Integer, Integer>, short[]> call(Tuple2<Tuple2<Integer, Integer>, short[]> file) throws Exception {
			short[] tab = file._2;
			for (int i = 0; i < tab.length; i++){
				if (tab[i] < 0)
					tab[i] = 0;
				if (tab[i] > 255)
					tab[i] = 255;
			}
			return new Tuple2<Tuple2<Integer, Integer>, short[]>(file._1, tab);
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
			
			int yOriginInBigImage = yTile * tileSize;
			int xOriginInBigImage = xTile * tileSize;
			
			int y_min = yOriginInBigImage % smallTileSize;
			int x_min = xOriginInBigImage % smallTileSize;
			
			int y_max = tileSize + y_min;
			int x_max = tileSize + x_min;
			
			Tuple2<Integer, Integer> tmpImgY = y_max <= (5 * smallTileSize)? new Tuple2<Integer, Integer>(minTgtPerTile, minSizeTmpImage): new Tuple2<Integer, Integer>(maxTgtPerTile, maxSizeTmpImage);
			Tuple2<Integer, Integer> tmpImgX = x_max <= (5 * smallTileSize)? new Tuple2<Integer, Integer>(minTgtPerTile, minSizeTmpImage): new Tuple2<Integer, Integer>(maxTgtPerTile, maxSizeTmpImage);

			// here we start the big work
			BufferedImage image = new BufferedImage(tmpImgX._2, tmpImgY._2, BufferedImage.TYPE_INT_ARGB);
			
			short[] heights = file._2;
			int a = 0;
			
			for (int j = y_min; j < y_max; j++) {
				for (int i = x_min; i < x_max; i++) {
					image.setRGB(i, j, getColor(heights[a]));
					a++;
				}
			}
			
			for (int j = 0; j < tmpImgY._1; j++){
				for (int i = 0; i < tmpImgX._1; i++) {
					int indexY = yOriginInBigImage / smallTileSize + j;
					int indexX = xOriginInBigImage / smallTileSize + i;
					BufferedImage subImage = image.getSubimage(i * smallTileSize, j * smallTileSize, smallTileSize, smallTileSize);
					byte[] subImageInBytes = bufferedImageToByteStream(subImage);
					res.add(new Tuple2<String, byte[]>(indexY + "-" + indexX, subImageInBytes));
				}
			}
			
			return res.iterator();
		}
	};
	
	public static final int getColor(short height) {
		int r = 255, g = 255, b = 255;
		if(height == 0){
			r = 14;
			g = 93;
			b = 183;
		} else if((height > 0) && (height <= 85)) {
			r = 0;
			g = 85 + height*2;
			b = 85;
		} else if(height > 85 && height <= 170) {
			r = 85 + height * 2;
			g = 255;
			b = 85;
		}else if(height > 170 && height <= 254) {
			r = 220 - height;
			g = 220 - height;
			b = 110 - height;
		}else if(height == 255) {
			r = 80;
			g = 80;
			b = 10;
		}
		return (255<<24) | (r<<16) | (g<<8) | b;
	}
	
	public static final byte[] bufferedImageToByteStream(BufferedImage image) {
		byte[] imageInByte = null;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ImageIO.write(image, "png", baos);
			baos.flush();
			imageInByte = baos.toByteArray();
			baos.close();
		} catch (IOException e){
			e.printStackTrace();
		}
		return imageInByte;
	}
	
	public static final BufferedImage byteStreamToBufferedImage(byte[] stream) {
		ByteArrayInputStream bais = new ByteArrayInputStream(stream);
		BufferedImage subImage = null;
		try {
			subImage = ImageIO.read(bais);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return subImage;
	}
	
	// we combine all the images with the same key
	public static Function2<byte[], byte[], byte[]> combineImagesWithSameKey = new Function2<byte[], byte[], byte[]>() {

		private static final long serialVersionUID = 2044592339629795644L;

		@Override
		public byte[] call(byte[] oneBytes, byte[] twoBytes) throws Exception {
			BufferedImage oneImage = byteStreamToBufferedImage(oneBytes);
			BufferedImage twoImage = byteStreamToBufferedImage(twoBytes);
			
			
			BufferedImage combinedImage = new BufferedImage(smallTileSize, smallTileSize, BufferedImage.TYPE_INT_ARGB);
			
			Graphics g = combinedImage.getGraphics();
			g.drawImage(oneImage, 0, 0, null);
			g.drawImage(twoImage, 0, 0, null);
			
			return bufferedImageToByteStream(combinedImage);
		}
		
	};
	
	// convert tile in short[] format to RGB BufferedImage
	public static PairFunction<Tuple2<Tuple2<Integer, Integer>, short[]>, Tuple2<Integer, Integer>, BufferedImage> toBufferedImage = new PairFunction<Tuple2<Tuple2<Integer, Integer>, short[]>, Tuple2<Integer, Integer>, BufferedImage>() {

		private static final long serialVersionUID = -5953046102975386641L;

		@Override
		public Tuple2<Tuple2<Integer, Integer>, BufferedImage> call(Tuple2<Tuple2<Integer, Integer>, short[]> file) throws Exception {
			BufferedImage image = new BufferedImage(tileSize, tileSize, BufferedImage.TYPE_INT_ARGB);
			
			short[] heights = file._2;
			int a = 0;
			
			for (int j = 0; j < tileSize; j++) {
				for (int i = 0; i < tileSize; i++) {
					image.setRGB(i, j, getColor(heights[a]));
					a++;
				}
			}

			return new Tuple2<Tuple2<Integer, Integer>, BufferedImage>(file._1, image);
		}
	};
	    
	// gives tiles a key corresponding to their future position
	public static PairFunction<Tuple2<String, byte[]>, String, ZoomTile> zoomOut1Map = new PairFunction<Tuple2<String, byte[]>, String, ZoomTile>() {
		private static final long serialVersionUID = 6295049499770038005L;

		@Override
		public Tuple2<String, ZoomTile> call(Tuple2<String, byte[]> file) throws Exception {

			String coords[] = file._1.split("-");
			
	        int yTileForBase = Integer.parseInt(coords[0]);
	        int xTileForBase = Integer.parseInt(coords[1]);
	        	        	        
			int yTileForTarget = yTileForBase / 2;
			int xTileForTarget = xTileForBase / 2;
			
			String coordsTargetLabel = yTileForTarget + "-" + xTileForTarget;

			int yPos = yTileForBase%2 == 0? 0: 256;
			int xPos = xTileForBase%2 == 0? 0: 256;

			return new Tuple2<String, ZoomTile>(coordsTargetLabel, new ZoomTile(file._2, xTileForTarget, yTileForTarget, xPos, yPos));
		}
	};
	
	// we combine all the images with the same key
	public static Function2<ZoomTile, ZoomTile, ZoomTile> zoomOut1Reducer = new Function2<ZoomTile, ZoomTile, ZoomTile>() {

		private static final long serialVersionUID = 2044592339629795644L;

		@Override
		public ZoomTile call(ZoomTile z1, ZoomTile z2) throws Exception {
			BufferedImage image1 = byteStreamToBufferedImage(z1.getImage());
			BufferedImage image2 = byteStreamToBufferedImage(z2.getImage());

			BufferedImage combinedImage = new BufferedImage(512, 512, BufferedImage.TYPE_INT_ARGB);

			Graphics g = combinedImage.getGraphics();
			g.drawImage(image1, z1.getxPos(), z1.getyPos(), null);
			g.drawImage(image2, z2.getxPos(), z2.getyPos(), null);
			
			return new ZoomTile(bufferedImageToByteStream(combinedImage), 0, 0, 0, 0);
		}
	};
	
	public static final int realSizeTile(int zoomOutIndex) {
		return (int) 256 * (int) Math.pow(2, zoomOutIndex);
	}
}