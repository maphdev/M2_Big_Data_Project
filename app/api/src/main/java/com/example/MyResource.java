package com.example;

import org.apache.commons.configuration.ConfigurationFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.util.ToolRunner;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

/**
 * Root resource (exposed at "myresource" path)
 */
@Path("")
public class MyResource implements ServletContextListener {
    @GET
    @Path("/tiles/{x}/{y}/{z}")
    @Produces("image/png")
    public Response getImage(@PathParam("x") String x, @PathParam("y") String y, @PathParam("z") String z) throws IOException {
        HBase.setUp();

        byte[] image = HBase.getImageFromHBase(x, y, z);

        return Response.ok(image).build();
    }

    public static Connection con;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.0.5.25");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            con = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        try {
            con.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
