package bigdata;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBase extends Configured {		
	private static final byte[] TABLE_NAME = Bytes.toBytes("team-rocket");
    public static final byte[] ZOOM_0_FAMILY = Bytes.toBytes("zoom_0");
    public static final byte[] ZOOM_1_FAMILY = Bytes.toBytes("zoom_1");
    public static final byte[] ZOOM_2_FAMILY = Bytes.toBytes("zoom_2");
    public static final byte[] ZOOM_3_FAMILY = Bytes.toBytes("zoom_3");
    public static final byte[] ZOOM_4_FAMILY = Bytes.toBytes("zoom_4");
    private static final byte[] TILE_COLUMN = Bytes.toBytes("tile");
    
    private static Connection connection;
    private static Table table;
    
    public static void setUp() throws IOException{
    	Configuration conf = HBaseConfiguration.create();
  		connection = ConnectionFactory.createConnection(conf);
  		table = connection.getTable(TableName.valueOf(TABLE_NAME));
    }
	
    // create table
    public static void createTable() {
        try {
            final Admin admin = connection.getAdmin();
            
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            HColumnDescriptor zoom0 = new HColumnDescriptor(ZOOM_0_FAMILY);
            HColumnDescriptor zoom1 = new HColumnDescriptor(ZOOM_1_FAMILY);
            HColumnDescriptor zoom2 = new HColumnDescriptor(ZOOM_2_FAMILY);
            HColumnDescriptor zoom3 = new HColumnDescriptor(ZOOM_3_FAMILY);
            HColumnDescriptor zoom4 = new HColumnDescriptor(ZOOM_4_FAMILY);
            
            tableDescriptor.addFamily(zoom0);
            tableDescriptor.addFamily(zoom1);
            tableDescriptor.addFamily(zoom2);
            tableDescriptor.addFamily(zoom3);
            tableDescriptor.addFamily(zoom4);
            
            createOrOverwrite(admin, tableDescriptor);
            
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
    
	private static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }
	
	// Add a row (tile)
	public static void addRow(String tileName, byte[] data, byte[] family) throws Exception {		
		Put put = new Put(Bytes.toBytes(tileName));
		put.addColumn(family, TILE_COLUMN, data);
		table.put(put);
	}
	
	public static int numberRowsPerFamily(byte[] family) throws IOException {
		Scan scan = new Scan();
	    scan.addFamily(family);
	    
	    ResultScanner resultScanner = table.getScanner(scan);
	    Iterator<Result> iterator = resultScanner.iterator();
	    
	    int i = 0;
	    while (iterator.hasNext())
	    {
	        iterator.next();
	        i+= 1;
	    }
	    return i;
	}
	
	public static BufferedImage saveImageFromHBase() throws IOException {
		String tileName = "104-419";
		byte[] row = Bytes.toBytes(tileName);
		
		Get get = new Get(row);
		get.addColumn(ZOOM_1_FAMILY, TILE_COLUMN);
		
		System.out.println("hello");
		
		Result res = table.get(get);
		byte[] tile = res.getValue(ZOOM_1_FAMILY, TILE_COLUMN);
		
		System.out.println(tile);
		
		return Utils.byteStreamToBufferedImage(tile);
	}
}
