package bigdata;

import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Transparency;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import javax.imageio.ImageIO;
import scala.Tuple2;

public class Utils {
	public static final int tileSize = 1201;
	public static final int smallTileSize = 256;
	public static final int halfNumberTilesLong = 90;
	public static final int halfNumberTilesLat = 180;
	
	public static Tuple2<Integer, Integer> getTileCoordsByFileName (String path) {
		String fullFileName = path.substring(path.lastIndexOf("/")+1);
		String fileName = fullFileName.substring(0, fullFileName.lastIndexOf("."));
		    
        String longLabel = fileName.substring(0, 1).toLowerCase();
		Integer longValue = Integer.parseInt(fileName.substring(1, 3));
		
		String latLabel = fileName.substring(3, 4).toLowerCase();
		Integer latValue = Integer.parseInt(fileName.substring(4, 7));
		
		if (longLabel.equals("n")) {
			longValue = Utils.halfNumberTilesLong - longValue - 1;
		} else if (longLabel.equals("s")) {
			longValue = Utils.halfNumberTilesLong + longValue - 1;
		}
		
		if (latLabel.equals("w")) {
			latValue = Utils.halfNumberTilesLat - latValue;
		} else if (latLabel.equals("e")) {
			latValue = Utils.halfNumberTilesLat + latValue;
		}
		
		return new Tuple2<Integer, Integer>(longValue, latValue);
	}
	
	public static short[] convertByteArrayToShortArray (byte[] bytes) {
		short[] shorts = new short[tileSize * tileSize];
		if (shorts.length == bytes.length/2){
			ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asShortBuffer().get(shorts).rewind();
		}
		for (int i = 0; i < shorts.length; i++){
			if (shorts[i] < 0)
				shorts[i] = 0;
			/*
			if (shorts[i] > 255)
				shorts[i] = 255;
			*/
		}
		return shorts;
	}
	
	public static final int getColor(short height) {
		int r = 255, g = 255, b = 255;
		if(height == 0){
			r = 14;
			g = 93;
			b = 183;
		} else if((height > 0) && (height <= 255)) {
			r = 0;
			g = 85 + height * 2/3;
			b = 85;
		} else if(height > 85 && height <= 1020) {
			r = 85 + height / 6;
			g = 255;
			b = 85;
		}else if(height > 1020 && height <= 2040) {
			r = 255 - height / 8;
			g = 255 - height / 8;
			b = 85 - height / 24;
		}else if(height > 2040 && height <= 3060) {
			r = 0 + height / 12;
			g = 0 + height / 12;
			b = 0 + height / 12;
		} else if(height > 3600) {
			r = 255;
			g = 255;
			b = 255;
		}
		return (255<<24) | (r<<16) | (g<<8) | b;
	}
	
	public static byte[] bufferedImageToByteStream(BufferedImage image) {
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
	
	public static BufferedImage resizeImage (BufferedImage image, int areaWidth, int areaHeight) {
	    float scaleX = (float) areaWidth / image.getWidth();
	    float scaleY = (float) areaHeight / image.getHeight();
	    float scale = Math.min(scaleX, scaleY);
	    int w = Math.round(image.getWidth() * scale);
	    int h = Math.round(image.getHeight() * scale);
	
	    int type = image.getTransparency() == Transparency.OPAQUE ? BufferedImage.TYPE_INT_RGB : BufferedImage.TYPE_INT_ARGB;
	
	    boolean scaleDown = scale < 1;
	
	    if (scaleDown) {
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
}
