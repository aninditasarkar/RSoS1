/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.cassandraDeletion;

import ac.at.ju.storageUnit.CassandraConnector;
import ac.at.ju.storageUnit.Configuration;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.List;
import org.json.simple.JSONObject;

/**
 *
 * @author anindita
 */
public class DeletionCondition {
    JSONObject conditionjson;
    private List keyList=new ArrayList();
    CassandraConnector cassandraObj;
    private String partitionKey;

    public void setCassandraObj(CassandraConnector cassandraObj) {
        this.cassandraObj = cassandraObj;
       
    }
    
    //receive json object under "eliminate" key tag 
    public void setConditionjson(JSONObject conditionjson) {
        this.conditionjson = conditionjson;
    }
    
    public void setKeyList() {
      
        keyList.addAll(this.conditionjson.keySet());   //retrieve keyset from jsonobject and transfer set to list           
       }
    
    public String getKeyQuery(int i) {
        return keyList.get(i).toString();   //get key name from specific position
    }

    public String getValueQuery(int i) {
        return this.conditionjson.get(keyList.get(i).toString()).toString(); //get value of specific positioned key
    }

    //receive partition key which is in one number by running cql
    public void addPartitionKey() {
        //cassandraConnect();
        this.partitionKey= this.cassandraObj.getPartitionKey();
    }

    //checking the received key(collecting from input json object) will be matched with partition key or not
    public boolean isKey(String conditionKey)
    {
        boolean check=false;
        if(conditionKey.equalsIgnoreCase(this.partitionKey))
            {
               check=true; 
            }
        return check;
    }
    
    //If received key(collecting from input json object) is only one and it is single 
    //in input json object the the deletion condition
    public String getKeyDeleteCondition()
    {
       return (partitionKey+ " in ('"+conditionjson.get(partitionKey).toString()+"')");
    }
    
    //for running the selection query of cql then the sub part of condition generating method
    public String getSelectCondition(int position)
    {
        return (getKeyQuery(position)+"='"+getValueQuery(position)+"'");
    }
    
    //If received key(collecting from input json object) is not the partition key
    //then the deletion condition
    public String getnonKeyDeleteCondition()
    {
        //String condition="";
    
          //cassandraObj.setCondition(getSelectCondition(0));
          cassandraObj.setCondition(getNonKeySelectCondition());
          cassandraObj.setColumnName(partitionKey);
          List<Row> results=cassandraObj.viewConditionColumnData();
          String conditionValue="'"+results.get(0).getString(partitionKey)+"'";
          for(int i=1;i<results.size();i++)
          //for(Row row : results)
          {
              conditionValue+=", '"+results.get(i).getString(partitionKey)+"'";
          }
          System.out.println("from getnonKeyDeleteCondition="+partitionKey+ " in ("+conditionValue+")");
      return (partitionKey+ " in ("+conditionValue+")"); //for testing purpose
    }
    
    //If received key(collecting from input json object) is not the partition key and 
    //need to know about the corresponding partion key value. Then running the select cql query
    //, the generated condition
    public String getNonKeySelectCondition()
    {
        String condition=getSelectCondition(0);
        for(int i=1;i<keyList.size();i++)
        {
            condition+=" AND "+getSelectCondition(i);
        }
        return condition;
    }
    
    //checking of the existance of partion key in the received key list of input json object
    public boolean existKeyAttribute()
    {
        boolean check=false;
        for(int i=0;i<keyList.size();i++)
        {
        if(isKey(getKeyQuery(i)))
        {
            
          check =true;
          break;
        }
        
        
    }
       return check;
    }
    
    
    //the ultimate final condition generation method
    public String getCondition()
    {
        String condition="";
        setKeyList();
        addPartitionKey();
        if(keyList.size()==1)
        {
        if(isKey(getKeyQuery(0)))
        {
          condition=getKeyDeleteCondition();  
        }
        else
        {
            
          condition=getnonKeyDeleteCondition();
        }
        }
        else
        {
        if(existKeyAttribute())
        {
            condition=getKeyDeleteCondition();
        }
        
        
        else
        {
            condition=getnonKeyDeleteCondition();
        }
       
        }
        
        
        //this.cassandraObj.close();
        return condition;
    }
    
    }
