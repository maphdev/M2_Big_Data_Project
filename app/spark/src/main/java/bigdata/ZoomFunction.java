package bigdata;

import java.awt.Graphics;
import java.awt.image.BufferedImage;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ZoomFunction {
	
	private final static int tmpTileSize = 512;
	
	// gives tiles a key corresponding to their future position in the zoomed tile
	public static PairFunction<Tuple2<String, byte[]>, String, ZoomTile> zoomMap = new PairFunction<Tuple2<String, byte[]>, String, ZoomTile>() {
		private static final long serialVersionUID = 6295049499770038005L;
	
		@Override
		public Tuple2<String, ZoomTile> call(Tuple2<String, byte[]> file) throws Exception {
	
			String coords[] = file._1.split("-");
			
	        int yTileForBase = Integer.parseInt(coords[0]);
	        int xTileForBase = Integer.parseInt(coords[1]);
	        	        	        
			int yTileForTarget = yTileForBase / 2;
			int xTileForTarget = xTileForBase / 2;
			
			String coordsTargetLabel = yTileForTarget + "-" + xTileForTarget;
			
			int yPos = yTileForBase%2 == 0? 0: Utils.smallTileSize;
			int xPos = xTileForBase%2 == 0? 0: Utils.smallTileSize;
			
			return new Tuple2<String, ZoomTile>(coordsTargetLabel, new ZoomTile(file._2, xTileForTarget, yTileForTarget, xPos, yPos, false));
		}
	};
	
	// we combine all the images with the same key in a big image
	public static Function2<ZoomTile, ZoomTile, ZoomTile> zoomReducer = new Function2<ZoomTile, ZoomTile, ZoomTile>() {
	
		private static final long serialVersionUID = 2044592339629795644L;
	
		@Override
		public ZoomTile call(ZoomTile z1, ZoomTile z2) throws Exception {
			BufferedImage image1 = Utils.byteStreamToBufferedImage(z1.getImage());
			BufferedImage image2 = Utils.byteStreamToBufferedImage(z2.getImage());
	
			BufferedImage combinedImage = new BufferedImage(tmpTileSize, tmpTileSize, BufferedImage.TYPE_INT_ARGB);
	
			Graphics g = combinedImage.getGraphics();
			g.drawImage(image1, z1.getxPos(), z1.getyPos(), null);
			g.drawImage(image2, z2.getxPos(), z2.getyPos(), null);		
			
			return new ZoomTile(Utils.bufferedImageToByteStream(combinedImage), 0, 0, 0, 0, true);
		}
	};
	
	// we resize the pictures 512 -> 256 (and sometimes correct the tiles that were alone in they group key)
	public static PairFunction<Tuple2<String, ZoomTile>, String, byte[]> zoomResize = new PairFunction<Tuple2<String, ZoomTile>, String, byte[]>() {
	
		private static final long serialVersionUID = 2044592339629795646L;
	
		@Override
		public Tuple2<String, byte[]> call(Tuple2<String, ZoomTile> file) throws Exception {
			BufferedImage image = Utils.byteStreamToBufferedImage(file._2.getImage());
			
			if (file._2.isProcessedInMap() == false) {				
				BufferedImage combinedImage = new BufferedImage(tmpTileSize, tmpTileSize, BufferedImage.TYPE_INT_ARGB);
				Graphics g = combinedImage.getGraphics();
				g.drawImage(image, file._2.getxPos(), file._2.getyPos(), null);
				return new Tuple2<String, byte[]>(file._1, Utils.bufferedImageToByteStream(Utils.resizeImage(combinedImage, Utils.smallTileSize, Utils.smallTileSize)));
			} else {
				return new Tuple2<String, byte[]>(file._1, Utils.bufferedImageToByteStream(Utils.resizeImage(image, Utils.smallTileSize, Utils.smallTileSize)));
			}
		}
	};
}
