package ac.at.ju.rest;
 
import ac.at.ju.cassandraDeletion.DeletionValue;
import ac.at.ju.cassandraView.JSONViewTable;
import ac.at.ju.cassandraView.ViewTable;
import ac.at.ju.storageUnit.DatasetEntry;
import com.datastax.driver.core.Row;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
 
@Path("/")
public class CassandraService {
 
	@GET
	@Path("/{param}")      //example param=u008&U010_ecg_123456789
	public Response getMsg(@PathParam("param") String msg) {
            
                List<Row> value=ViewTable.getInstance().mainGetTableData(msg);
                String table="<!DOCTYPE html>\n" +
                             "<html>\n" +
                             "<head>\n"+
                             "<title>The information related of </title>\n"+
                            "<meta charset=\"UTF-8\">\n"+
                            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n"+
                            "<link rel=\"stylesheet\" href=\"style.css\">"+
                            "</head>\n"+
                             "<body>\n"+
                             "<table border=\"1\" style=\"width:100%\">\n"+
                             "<tr>\n" +
                             "<td>time</td>\n" +
                             "<td>data</td>\n" +
                             "<td>deviceid</td>\n" +
                             "</tr>\n";
                              
                for(int i=0;i<value.size();i++)
                  {
                      table+="<tr>\n";
                      Row tempRow=value.get(i);
                    table+="<td>\n"+tempRow.getString("time")+"</td>\n";
                    table+="<td>\n"+tempRow.getString("data")+"</td>\n";
                    table+="<td>\n"+tempRow.getString("deviceid")+"</td>\n";
                    table+="</tr>\n";
                    
                  }
                table+="</table>\n" +
                        "</body>\n" +
                        "</html>";
                //String output = "Jersey say : " + msg+"   hello===="+new DatasetEntry().mainReadJSONObject();
 
		return Response.status(200).entity(table).build();
                
 
	}
        
     @POST
     @Path("receive")
     //@Consumes(MediaType.APPLICATION_JSON)
     //@Produces(MediaType.APPLICATION_JSON)
   public Response consumeJSON(String request) throws Exception{
        String output = new DatasetEntry().mainReadJSONObject(request);
       return Response.status(200).entity(output).build();
                       
   }
   
   @POST
     @Path("visual")
     //@Consumes(MediaType.APPLICATION_JSON)
     //@Produces(MediaType.APPLICATION_JSON)
   public Response visualizeData(String request) throws Exception{
       String value=JSONViewTable.getInstance().getTabledata(request);
       return Response.status(200).entity(value).build();
                       
   }
   
   @POST
    @Path("delete")
    
    public String removeData(String requestJSON)throws Exception
    {
        return (DeletionValue.getInstance().deleteTableRow(requestJSON));
    }

 
}