/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.cassandraView;

import ac.at.ju.storageUnit.CassandraConnector;
import ac.at.ju.storageUnit.Configuration;
import ac.at.ju.storageUnit.DatasetEntry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.json.simple.JSONObject;

/**
 *
 * @author anindita
 */
public class JSONViewTable {
    CassandraConnector obj=new CassandraConnector();
    private JSONObject requestJSONObj;
    private JSONObject objCondition;
    private String queryValue="";
    private List keyList=new ArrayList();
    //private String keyQuery;
    //private String valueQuery;
    
    public static JSONViewTable getInstance()
    {
        return new JSONViewTable();
    }

    //conversion from jsonString to jsonobject
    public void addRequestJSONObj(String requestjson) {
        this.requestJSONObj = new DatasetEntry().jsonStringtoObject(requestjson);
    }

    public String getKeyspaceName() {
        return requestJSONObj.get("account").toString();
    }

    public String getColumnfamilyName() {
        return requestJSONObj.get("container").toString();
    }

    //draw the condition string for retrieve value set
    public void setKeyList() {
        objCondition= (JSONObject)requestJSONObj.get("condition"); //retrieve object under "condition" key
        keyList.addAll(objCondition.keySet());   //retrieve keyset from jsonobject andtransfer set to list           
       }
    
    public String getKeyQuery(int i) {
        return keyList.get(i).toString();   //get key name from specific position
    }

    public String getValueQuery(int i) {
        return objCondition.get(keyList.get(i).toString()).toString(); //get value of specific positioned key
    }

    public String getCondition()
    {
        //String condition="";
        setKeyList();
        //if( keyList.size()==1 ) 
        //{
        //condition+=getKeyQuery(0)+"='"+getValueQuery(0)+"' ALLOW FILTERING"; //here assume every column type is varchar
        //}
        //else
        //{
         String condition=getKeyQuery(0)+"='"+getValueQuery(0)+"'";  
        
        for(int i=1;i<keyList.size();i++)
      {
        condition+=" AND "+getKeyQuery(i)+"='"+getValueQuery(i)+"'";
      }
       //condition+=" ALLOW FILTERING";
        //if( keyList.size()>=2 ) 
       //{
       //condition+=" AND "+getKeyQuery(keyList.size()-1)+"='"+getValueQuery(keyList.size()-1)+"' ALLOW FILTERING";
       //}
       // }
       System.out.println("condition="+condition);
        return condition;

    }
 //////////////
   
    ///Arrange the query result content in user readable format from row resultset
    public void addQueryValue(List<Row> value) {
        
        for(Row row : value)
        {
           ColumnDefinitions coldef=row.getColumnDefinitions();
           for(int i=0;i<coldef.size();i++)
            {
             queryValue+=coldef.getName(i)+"="+row.getString(coldef.getName(i))+" ";
            }
           queryValue+="\n";
       }
   }
    
    
  public String getTabledata(String requestjson)
    {
        addRequestJSONObj(requestjson);
        
        
        obj.connect(Configuration.getConfig("IP"), Integer.parseInt(Configuration.getConfig("PORT")));
        obj.setKeyspaceName(getKeyspaceName());
        obj.setTableName(getColumnfamilyName());
        obj.setCondition(getCondition());
        addQueryValue(obj.viewConditionData()); //it retrieves data content of a object means a row 
        //addQueryValue(obj.viewAllData());
        System.out.println("partition key="+obj.getPartitionKey()); ///temporary 
        obj.close();
        return queryValue;
    }
    
    public static void main(String []p)
    {
      String jsonForCassandraUser="{\n" +
"   \"account\":\"u103\",\n" +
"   \"container\":\"u009_ecg_12348\",\n" +

"   \"condition\":{\n" +
"      \"deviceid\":\"k009\",\n" +
//"      \"patientid\":\"p009\",\n" +
"   }\n" +
"}";
      JSONViewTable obj=new JSONViewTable();
      //obj.getTabledata(jsonForMongoUser);
      System.out.println("output="+obj.getTabledata(jsonForCassandraUser));
      //System.out.println("output="+obj.getCondition());
    }
    
}
