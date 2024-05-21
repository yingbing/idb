package idb.resource;

import idb.model.Message;
import idb.utils.JsonDatabaseHandler;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.util.Map;


@Path("/json")
public class JsonResource {
    private JsonDatabaseHandler jsonDatabaseHandler;

    public JsonResource(JsonDatabaseHandler jsonDatabaseHandler) {
        this.jsonDatabaseHandler = jsonDatabaseHandler;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Message getMessage() {
        return new Message("Hello, JSON!");
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createJson(Map<String, Object> json) {
        try {
            jsonDatabaseHandler.addJsonRecord(json, "User");
            return Response.status(Response.Status.CREATED).entity(json).build();
        } catch (IOException | ReflectiveOperationException e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GET
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJson(@PathParam("id") int id) {
        try {
            Map<String, Object> json = jsonDatabaseHandler.getJsonRecord(id, "User");
            if (json == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            return Response.ok(json).build();
        } catch (IOException | ReflectiveOperationException e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PUT
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateJson(@PathParam("id") int id, Map<String, Object> json) {
        try {
            jsonDatabaseHandler.updateJsonRecord(id, json, "User");
            return Response.ok(json).build();
        } catch (IOException | ReflectiveOperationException e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @DELETE
    @Path("/{id}")
    public Response deleteJson(@PathParam("id") int id) {
        try {
            jsonDatabaseHandler.getDatabase().getTable("Users").deleteRecord(id);
            return Response.noContent().build();
        } catch (IOException e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }
}
