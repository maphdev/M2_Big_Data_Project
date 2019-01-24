package bigdata;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class ZoomTile implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2113177953332563490L;
	private byte[] image;
	private int xTile;
	private int yTile;
	private int xPos;
	private int yPos;
	
	public ZoomTile(byte[] image, int xTile, int yTile, int xPos, int yPos) {
		this.image = image;
		this.xTile = xTile;
		this.yTile = yTile;
		this.xPos = xPos;
		this.yPos = yPos;
	}

	public byte[] getImage() {
		return image;
	}

	public void setImage(byte[] image) {
		this.image = image;
	}

	public int getxTile() {
		return xTile;
	}

	public void setxTile(int xTile) {
		this.xTile = xTile;
	}

	public int getyTile() {
		return yTile;
	}

	public void setyTile(int yTile) {
		this.yTile = yTile;
	}

	public int getxPos() {
		return xPos;
	}

	public void setxPos(int xPos) {
		this.xPos = xPos;
	}

	public int getyPos() {
		return yPos;
	}

	public void setyPos(int yPos) {
		this.yPos = yPos;
	}
	
	 private void writeObject(ObjectOutputStream os)
	            throws IOException {
		 os.writeInt(xTile);
		 os.writeInt(yTile);
		 os.writeInt(xPos);
		 os.writeInt(yPos);
		 os.writeObject(image);
	 }

	 private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
		 this.xTile = is.readInt();
		 this.yTile = is.readInt();
		 this.xPos = is.readInt();
		 this.yPos = is.readInt();
		 this.image = (byte[])is.readObject();
		 
	    }
}
