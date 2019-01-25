package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;

import ch.epfl.lamp.compiler.msil.util.Table;

public class HBase {
	private static final byte[] TABLE_NAME = Bytes.toBytes("team-rocket");
    private static final byte[] ZOOM_0_FAMILY = Bytes.toBytes("zoom_0");
    private static final byte[] NAME_COLUMN = Bytes.toBytes("name");
    private static final byte[] TILE_COLUMN = Bytes.toBytes("tile");

	public static class Create extends Configured implements Tool {
		
		public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
	        if (admin.tableExists(table.getTableName())) {
	            admin.disableTable(table.getTableName());
	            admin.deleteTable(table.getTableName());
	        }
	        admin.createTable(table);
	    }

	    public static void createTable(Connection connect) {
	        try {
	            final Admin admin = connect.getAdmin();
	            
	            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
	            HColumnDescriptor zoom0 = new HColumnDescriptor(ZOOM_0_FAMILY);
	            
	            tableDescriptor.addFamily(zoom0);
	            
	            createOrOverwrite(admin, tableDescriptor);
	            
	            admin.close();
	        } catch (Exception e) {
	            e.printStackTrace();
	            System.exit(-1);
	        }
	    }

	    public int run(String[] args) throws IOException {
	        Connection connection = ConnectionFactory.createConnection(getConf());
	        createTable(connection);
	        return 0;
	    }
	}
	
    
}
