package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;

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
@Path("api")
public class MyResource {

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     *
     * @return String that will be returned as a text/plain response.
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getIt() {
        return "Got it!";
    }

    @GET
    @Path("/tiles/{x}/{y}/{z}")
    @Produces("image/png")
    public Response getImage(@PathParam("x") String x, @PathParam("y") String y, @PathParam("z") String z) throws IOException {
        HBase.setUp();
        //byte[] tiles = HBase.getImageFromHBase("418", "104", "1");
        //return Response.ok(tiles).build();

        byte[] tiles = HBase.getImageFromHBase(x,y,z);
        if (tiles == null) {
            return Response.ok(HBase.getImageFromHBase(Integer.toString(836), Integer.toString(207), Integer.toString(0))).build();
        } else {
            return Response.ok(tiles).build();
        }
    }
}
