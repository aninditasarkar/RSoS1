/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.databaseDesign;

import ac.at.ju.configuration.LogGeneration;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 *
 * @author anindita
 */
public class StringConversion {
    public JSONObject jsonObj=new JSONObject();
    public String remoteString="";
    public static StringConversion getInstance()
    {
        return new StringConversion();
    }

    public void setJsonObj(JSONObject jsonObj) {
        this.jsonObj = jsonObj;
    }

    public void setRemoteString(String remoteString) {
        this.remoteString = remoteString;
    }
    
    public JSONObject jsonStringtoObject(String jsonString)
    {
        JSONObject requestObj = new JSONObject();
        
         try {
            System.out.println("StringConversion.while.jsonStringtoObject="+jsonString);
            requestObj = (JSONObject) new JSONParser().parse(jsonString);
            } catch (Exception e) {
            //logger.log(Level.SEVERE, null, e);
            System.out.println("DatasetEntry Exception=" + e);
        }
         return requestObj;
    }
    
    //this method calls jsonStringtoObject, setJsonObj, setRemoteString method
    //Needs to call this method to access all the methods
    //Another way is possible but it makes the code more complex
   public void mainReplaceStringtoObject(String inputjsonString)
    {
         
        //LogGeneration.init();
        //Logger logger = Logger.getLogger(LogGeneration.class.getName());
        System.out.println("StringConversion.jsonStringtoObject="+inputjsonString);
        
        if(inputjsonString.contains("\"remotefiledata\":"))
            {
                int index1=inputjsonString.indexOf("\"remotefiledata\":");
                int index2=inputjsonString.indexOf("\"id\":");   //temporary purpose
            //int index2=inputjsonString.indexOf("\"fileextension\":");
            System.out.println("StringConversion.mainReplaceStringtoObject.index2="+index2);
            String replaceString=inputjsonString.substring(index1, index2);
            setRemoteString(replaceString);
            
            //because jsonstring contains with another character can not be converted to the jsonobject. 
            String replaceJSONString=inputjsonString.replace(replaceString, "\"remotefiledata\": \"REMOTE123\",");  
            
            
            //String replaceJSONString=jsonString.replaceAll("/\n/g", "\\n").
              //      replaceAll("/\\r/g", "\\r").replaceAll("/\\t/g", "\\t").
                //    replaceAll("/\\f/g","\\f");
                setJsonObj(jsonStringtoObject(replaceJSONString));
                System.out.println("HadoopRestServer.StringConversion.jsonobject="+replaceJSONString);
                
            }
            else
            {
                
                 setJsonObj(jsonStringtoObject(inputjsonString));
            }
        
       
        
        
    }
    
    
    
     
}
