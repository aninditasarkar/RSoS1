/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.configuration;


import ac.at.ju.databaseDesign.HadoopJSONServer;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PUT;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * REST Web Service
 *
 * @author anindita
 * 
 * both storage and read purpose POST interface is used for temporary purpose.
 * It need to modify in future. different interface need to use for different purpose.
 *
 * 
 * 
 */
@Path("rest")
public class RestResource {

    @Context
    private UriInfo context;

    /**
     * Creates a new instance of RestResource
     */
    public RestResource() {
    }

    /**
     * Retrieves representation of an instance of ac.at.ju.dataStore.RestResource
     * @return an instance of java.lang.String
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getJson() {
        //TODO return proper representation object
        //return "{ \"account\":\"U009\"}";
        return "Welcome in HadoopRestServer";
        //throw new UnsupportedOperationException();
    }
    
    /**
     * PUT method for updating or creating an instance of RestResource
     * @param content representation for the resource
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    public void putJson(String content) {
    }
    
    @POST
    @Path("visual")
    
    public String visualizeData(String requestJSON)throws Exception
    {
        return new HadoopJSONServer().hadoopRead(requestJSON);
        //return "hello";
    }
    
    @POST
    @Path("delete")
    
    public String removeData(String requestJSON)throws Exception
    {
        return new HadoopJSONServer().hadoopDelete(requestJSON);
        //return "hello";
    }

    
     @POST
     @Path("receive")
     //@Consumes(MediaType.APPLICATION_JSON)
     //@Produces(MediaType.APPLICATION_JSON)
   public Response consumeJSON(String request) throws Exception{
         String output =new HadoopJSONServer().hadoopStorage(request);
        return Response.status(200).entity(output).build();
                               
   }
}
