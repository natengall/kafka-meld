package com.github.meld;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class IndexController {

    String[] actions = { "connect", "kafka" };

    public IndexController() {
    }

    @GET
    public Response getIndex() {
        return Response.ok(actions).build();
    }

}