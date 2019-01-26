package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

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

    public static void setUp() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.0.5.25");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(conf);
        table = connection.getTable(TableName.valueOf(TABLE_NAME));
    }

    public static byte[] getImageFromHBase(String x, String y, String z)  {
        //String tileName = "104-419";
        String tileName = y+"-"+x;
        byte[] row = Bytes.toBytes(tileName);

        Get get = new Get(row);

        byte[] family;
        switch (z) {
            case "0":
                family = ZOOM_0_FAMILY;
                break;
            case "1":
                family = ZOOM_1_FAMILY;
                break;
                /*
            case "2":
                family = ZOOM_2_FAMILY;
                break;
                */
            default:
                return null;
        }

        get.addColumn(family, TILE_COLUMN);

        Result res = null;
        try {
            res = table.get(get);
            if (res.isEmpty()) {
                return HBase.getImageFromHBase("418", "104", "1");
            } else {
                byte[] tile = res.getValue(family, TILE_COLUMN);
                return tile;
            }
        } catch (IOException e) {
            return HBase.getImageFromHBase("418", "104", "1");
        }
    }
}
