/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.cassandraDeletion;

import ac.at.ju.cassandraView.JSONViewTable;
import ac.at.ju.storageUnit.CassandraConnector;
import ac.at.ju.storageUnit.Configuration;
import ac.at.ju.storageUnit.DatasetEntry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.List;
import org.json.simple.JSONObject;

/**
 *
 * @author anindita
 */
public class DeletionValue {
    //for deletion purpose cassandra delete on the basis of partition key not primary key
    //non primary key will never be placed in where clause.
    //present of non primary key with primary key in where clause, the query will not work. 
    //account name and container name will be in small case.
    
   //cassandra query language to retrieve the column name which is primary key.
    //select column_name from system_schema.columns where table_name='u009_ecg_12348' and kind='partition_key' allow filtering;
    
    //read the corresponding deviceid (primary key) value of patient id. 
    //select deviceid from u009_ecg_12348 where patientid='p007' allow filtering;
    
    //delete more than one row
    //delete from u009_ecg_12348 where deviceid in ('k001','k008');
    
    
     CassandraConnector obj=new CassandraConnector();
    private JSONObject requestJSONObj;
    private String container;
    private String rowIdentifier;
    
    public static DeletionValue getInstance()
    {
        return new DeletionValue();
    }

    //conversion from jsonString to jsonobject
    public void addRequestJSONObj(String requestjson) {
        this.requestJSONObj = new DatasetEntry().jsonStringtoObject(requestjson);
    }

    public String getKeyspaceName() {
        return requestJSONObj.get("account").toString().toLowerCase();
    }

    public String getColumnfamilyName() {
        this.container=requestJSONObj.get("container").toString().toLowerCase();
        return container;
    }

    //receive the deletion condition from DeletionCondition class
    public String getDeletionCondition()
    {
        DeletionCondition cond=new DeletionCondition();
         cond.setCassandraObj(obj);
        cond.setConditionjson((JSONObject)requestJSONObj.get("eliminate"));
        rowIdentifier=cond.getCondition();
        return rowIdentifier;
    }
    
    //delete data with the container if number of row is deletion after deleting requested row
    public String DataDeletion()
    {
        String value="";
        String deletionResult=obj.deleteRow();
        
        if(obj.getRowNumber()==0)
        {
            obj.deleteTable();
            value=container+" table is deleted with entire row of partition key "+rowIdentifier; 
        }
        else
        {
           value=deletionResult+rowIdentifier; 
        }
        return value;
    }
    //draw the condition string for retrieve value set
    
     ///Arrange the query result content in user readable format from row resultset
    
     public String deleteTableRow(String requestjson)
    {
        addRequestJSONObj(requestjson);
        
        
        obj.connect(Configuration.getConfig("IP"), Integer.parseInt(Configuration.getConfig("PORT")));
        obj.setKeyspaceName(getKeyspaceName());
        obj.setTableName(getColumnfamilyName());
        
        
        
        obj.setCondition(getDeletionCondition());
        
        String output=DataDeletion();
        obj.close();
        return output;
       //return cond.getCondition();
    }
     
     public static void main(String []p)
    {
      String jsonForCassandraUser="{\n" +
"   \"account\":\"u103\",\n" +
"   \"container\":\"test4\",\n" +

"   \"eliminate\":{\n" +
//"      \"patientid\":\"p4\",\n" +
//"      \"deviceid\":\"k4\"\n" +
             
              "      \"name\":\"ootp\",\n" +
              "      \"address\":\"k09\",\n" +
              //"      \"id\":\"5\"\n" +
              
"   }\n" +
"}";
      
      System.out.println("output="+DeletionValue.getInstance().deleteTableRow(jsonForCassandraUser));
      //System.out.println("output="+obj.getCondition());
    }
    
}
