package bigdata;

import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Transparency;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.imageio.ImageIO;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ZoomFunction {
	
	private static int zoomLevel = 1;
	
	// gives tiles a key corresponding to their future position
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
			
			int yPos = yTileForBase%2 == 0? 0: getTileSizeForZoom(zoomLevel);
			int xPos = xTileForBase%2 == 0? 0: getTileSizeForZoom(zoomLevel);
			
			return new Tuple2<String, ZoomTile>(coordsTargetLabel, new ZoomTile(file._2, xTileForTarget, yTileForTarget, xPos, yPos, false));
		}
	};
	
	// we combine all the images with the same key
	public static Function2<ZoomTile, ZoomTile, ZoomTile> zoomReducer = new Function2<ZoomTile, ZoomTile, ZoomTile>() {
	
		private static final long serialVersionUID = 2044592339629795644L;
	
		@Override
		public ZoomTile call(ZoomTile z1, ZoomTile z2) throws Exception {
			BufferedImage image1 = byteStreamToBufferedImage(z1.getImage());
			BufferedImage image2 = byteStreamToBufferedImage(z2.getImage());
	
			BufferedImage combinedImage = new BufferedImage(512, 512, BufferedImage.TYPE_INT_ARGB);
	
			Graphics g = combinedImage.getGraphics();
			g.drawImage(image1, z1.getxPos(), z1.getyPos(), null);
			g.drawImage(image2, z2.getxPos(), z2.getyPos(), null);		
			
			return new ZoomTile(bufferedImageToByteStream(combinedImage), 0, 0, 0, 0, true);
		}
	};
	
	// convert PortableDataStream to short[]
	public static PairFunction<Tuple2<String, ZoomTile>, String, byte[]> zoomResize = new PairFunction<Tuple2<String, ZoomTile>, String, byte[]>() {
	
		private static final long serialVersionUID = 2044592339629795646L;
	
		@Override
		public Tuple2<String, byte[]> call(Tuple2<String, ZoomTile> file) throws Exception {
			BufferedImage image = byteStreamToBufferedImage(file._2.getImage());
			
			if (file._2.isResized() == false) {				
				BufferedImage combinedImage = new BufferedImage(512, 512, BufferedImage.TYPE_INT_ARGB);
				Graphics g = combinedImage.getGraphics();
				g.drawImage(image, file._2.getxPos(), file._2.getyPos(), null);
				
				return new Tuple2<String, byte[]>(file._1, bufferedImageToByteStream(resizeImage(combinedImage, 256, 256)));
			} else {
				return new Tuple2<String, byte[]>(file._1, bufferedImageToByteStream(resizeImage(image, 256, 256)));
			}
		}
		
	};

	// ----------> HELPERS
	public static final int getTileSizeForZoom(int zoomOutIndex) {
		return (int) 256 * (int) Math.pow(2, zoomOutIndex -1);
	}
	
	
	public static BufferedImage resizeImage (BufferedImage image, int areaWidth, int areaHeight) {
	    float scaleX = (float) areaWidth / image.getWidth();
	    float scaleY = (float) areaHeight / image.getHeight();
	    float scale = Math.min(scaleX, scaleY);
	    int w = Math.round(image.getWidth() * scale);
	    int h = Math.round(image.getHeight() * scale);
	
	    int type = image.getTransparency() == Transparency.OPAQUE ? BufferedImage.TYPE_INT_RGB : BufferedImage.TYPE_INT_ARGB;
	
	    boolean scaleDown = scale < 1;
	
	    if (scaleDown) {
	        // multi-pass bilinear div 2
	        int currentW = image.getWidth();
	        int currentH = image.getHeight();
	        BufferedImage resized = image;
	        while (currentW > w || currentH > h) {
	            currentW = Math.max(w, currentW / 2);
	            currentH = Math.max(h, currentH / 2);
	
	            BufferedImage temp = new BufferedImage(currentW, currentH, type);
	            Graphics2D g2 = temp.createGraphics();
	            g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
	            g2.drawImage(resized, 0, 0, currentW, currentH, null);
	            g2.dispose();
	            resized = temp;
	        }
	        return resized;
	    } else {
	        Object hint = scale > 2 ? RenderingHints.VALUE_INTERPOLATION_BICUBIC : RenderingHints.VALUE_INTERPOLATION_BILINEAR;
	
	        BufferedImage resized = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
	        Graphics2D g2 = resized.createGraphics();
	        g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, hint);
	        g2.drawImage(image, 0, 0, w, h, null);
	        g2.dispose();
	        return resized;
	    }
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
}
