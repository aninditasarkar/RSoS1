/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.cassandraView;

import ac.at.ju.storageUnit.CassandraConnector;
import com.datastax.driver.core.Row;
import java.util.List;

/**
 *
 * @author dsg
 */
public class ViewTable {
    private String uriMessage;
    private String keyspaceName;
    private String columnfamilyName;
    
    public static ViewTable getInstance()
    {
        return new ViewTable();
    }
    public void setURIMessage(String message)
    {
       this.uriMessage=message;
   }
     public String getKeyspaceName()
     {
         return uriMessage.substring(0, uriMessage.indexOf("&"));
     }
     
     public String getColumnfamilyName()
     {
         return uriMessage.substring(uriMessage.indexOf("&")+1);
     }
     
     public List<Row> mainGetTableData(String message)
     {
         setURIMessage(message);  //mandatory field
         
         CassandraConnector obj=new CassandraConnector();
         obj.connect("localhost", 9042);
         
         obj.setKeyspaceName(getKeyspaceName());
         obj.setTableName(getColumnfamilyName());
         List<Row> value=obj.viewAllData();
         /*String temp=new String();
         for(int i=0;i<value.size();i++)
         {
             temp+=value.get(i).toString();
         }*/
         //return temp;
         obj.close();
         
         return value;
         
     }
     /*public static void main(String []p)
     {
         
         String value=ViewTable.getInstance().mainGetTableData("u008&u010_ecg_123456789");
         System.out.println("view="+value);
         
     }*/
}
