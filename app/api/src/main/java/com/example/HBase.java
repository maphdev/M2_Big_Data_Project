package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBase extends Configured {
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

    private static Table table;

    public static void setUp() throws IOException {
        table = MyResource.con.getTable(TableName.valueOf(TABLE_NAME));
    }

    public static byte[] getImageFromHBase(String x, String y, String z)  {
        String tileName = y+"-"+x;
        byte[] row = Bytes.toBytes(tileName);
        byte[] family = getFamilyFromZoom(z);

        Get get = new Get(row);

        get.addColumn(family, TILE_COLUMN);

        Result res;
        byte[] tile = null;
        try {
            res = table.get(get);
            if (res.isEmpty()){
                tile = HBase.getImageFromHBase("418", "104", "10");
            } else {
                tile = res.getValue(family, TILE_COLUMN);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tile;
    }

    public static byte[] getFamilyFromZoom(String z) {
        byte[] family;
        switch (z) {
            case "0":
                family = ZOOM_11_FAMILY;
                break;
            case "1":
                family = ZOOM_10_FAMILY;
                break;
            case "2":
                family = ZOOM_9_FAMILY;
                break;
            case "3":
                family = ZOOM_8_FAMILY;
                break;
            case "4":
                family = ZOOM_7_FAMILY;
                break;
            case "5":
                family = ZOOM_6_FAMILY;
                break;
            case "6":
                family = ZOOM_5_FAMILY;
                break;
            case "7":
                family = ZOOM_4_FAMILY;
                break;
            case "8":
                family = ZOOM_3_FAMILY;
                break;
            case "9":
                family = ZOOM_2_FAMILY;
                break;
            case "10":
                family = ZOOM_1_FAMILY;
                break;
            case "11":
                family = ZOOM_0_FAMILY;
                break;
            default:
                return null;
        }
        return family;
    }
}
