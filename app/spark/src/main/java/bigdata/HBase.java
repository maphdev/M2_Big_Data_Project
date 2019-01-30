package bigdata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

public class HBase extends Configured implements Tool {
	private static final byte[] TABLE_NAME = Bytes.toBytes("team-rocket-ml");
    private static final byte[] ZOOM_0_FAMILY = Bytes.toBytes("zoom_0");
    private static final byte[] ZOOM_1_FAMILY = Bytes.toBytes("zoom_1");
    private static final byte[] ZOOM_2_FAMILY = Bytes.toBytes("zoom_2");
    private static final byte[] ZOOM_3_FAMILY = Bytes.toBytes("zoom_3");
    private static final byte[] ZOOM_4_FAMILY = Bytes.toBytes("zoom_4");
    private static final byte[] ZOOM_5_FAMILY = Bytes.toBytes("zoom_5");
    private static final byte[] ZOOM_6_FAMILY = Bytes.toBytes("zoom_6");
    private static final byte[] ZOOM_7_FAMILY = Bytes.toBytes("zoom_7");
    private static final byte[] ZOOM_8_FAMILY = Bytes.toBytes("zoom_8");
    private static final byte[] ZOOM_9_FAMILY = Bytes.toBytes("zoom_9");
    private static final byte[] ZOOM_10_FAMILY = Bytes.toBytes("zoom_10");
    private static final byte[] ZOOM_11_FAMILY = Bytes.toBytes("zoom_11");
    private static final byte[] TILE_COLUMN = Bytes.toBytes("tile");

    // create table
    private static void createTable(Connection connection) {
        try {
            final Admin admin = connection.getAdmin();

            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            HColumnDescriptor zoom0 = new HColumnDescriptor(ZOOM_0_FAMILY);
            HColumnDescriptor zoom1 = new HColumnDescriptor(ZOOM_1_FAMILY);
            HColumnDescriptor zoom2 = new HColumnDescriptor(ZOOM_2_FAMILY);
            HColumnDescriptor zoom3 = new HColumnDescriptor(ZOOM_3_FAMILY);
            HColumnDescriptor zoom4 = new HColumnDescriptor(ZOOM_4_FAMILY);
            HColumnDescriptor zoom5 = new HColumnDescriptor(ZOOM_5_FAMILY);
            HColumnDescriptor zoom6 = new HColumnDescriptor(ZOOM_6_FAMILY);
            HColumnDescriptor zoom7 = new HColumnDescriptor(ZOOM_7_FAMILY);
            HColumnDescriptor zoom8 = new HColumnDescriptor(ZOOM_8_FAMILY);
            HColumnDescriptor zoom9 = new HColumnDescriptor(ZOOM_9_FAMILY);
            HColumnDescriptor zoom10 = new HColumnDescriptor(ZOOM_10_FAMILY);
            HColumnDescriptor zoom11 = new HColumnDescriptor(ZOOM_11_FAMILY);

            tableDescriptor.addFamily(zoom0);
            tableDescriptor.addFamily(zoom1);
            tableDescriptor.addFamily(zoom2);
            tableDescriptor.addFamily(zoom3);
            tableDescriptor.addFamily(zoom4);
            tableDescriptor.addFamily(zoom5);
            tableDescriptor.addFamily(zoom6);
            tableDescriptor.addFamily(zoom7);
            tableDescriptor.addFamily(zoom8);
            tableDescriptor.addFamily(zoom9);
            tableDescriptor.addFamily(zoom10);
            tableDescriptor.addFamily(zoom11);

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
	public static void addRow(String tileName, byte[] data, int zoom) throws Exception {
		Configuration conf =  HBaseConfiguration.create();  
        conf.set("hbase.zookeeper.quorum", "10.0.5.25");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
  		Connection connection = ConnectionFactory.createConnection(conf);
  		Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
  		
		Put put = new Put(Bytes.toBytes(tileName));

		byte[] family = getFamilyFromZoom(zoom);

		put.addColumn(family, TILE_COLUMN, data);
		table.put(put);
	}
	
	public static void addZoomHbase(JavaPairRDD<String, byte[]> rdd, int zoom){
		rdd.foreachPartition(partition -> {
			Configuration conf =  HBaseConfiguration.create();  
	        conf.set("hbase.zookeeper.quorum", "10.0.5.25");
	        conf.set("hbase.zookeeper.property.clientPort", "2181");
	  		Connection connection = ConnectionFactory.createConnection(conf);
	  		Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
			
	  		while(partition.hasNext()){
	  			try {
	  				Tuple2<String, byte[]> file = partition.next();
					Put put = new Put(Bytes.toBytes(file._1));

					byte[] family = getFamilyFromZoom(zoom);

					put.addColumn(family, TILE_COLUMN, file._2);
					table.put(put);
				} catch (Exception e) {
					e.printStackTrace();
				}
	  		}
	  		
	  		connection.close();
		});
	}

	private static byte[] getFamilyFromZoom(int zoom){
		byte[] family = null;
		switch (zoom) {
		case 0:
			family = ZOOM_0_FAMILY;
			break;
		case 1:
			family = ZOOM_1_FAMILY;
			break;
		case 2:
			family = ZOOM_2_FAMILY;
			break;
		case 3:
			family = ZOOM_3_FAMILY;
			break;
		case 4:
			family = ZOOM_4_FAMILY;
			break;
		case 5:
			family = ZOOM_5_FAMILY;
			break;
		case 6:
			family = ZOOM_6_FAMILY;
			break;
		case 7:
			family = ZOOM_7_FAMILY;
			break;
		case 8:
			family = ZOOM_8_FAMILY;
			break;
		case 9:
			family = ZOOM_9_FAMILY;
			break;
		case 10:
			family = ZOOM_10_FAMILY;
			break;
		case 11:
			family = ZOOM_11_FAMILY;
			break;
		}
		return family;
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Connection connection = ConnectionFactory.createConnection(getConf());
		createTable(connection);
		connection.close();
  		return 0;
	}
}
