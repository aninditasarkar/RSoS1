/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.databaseDesign;


import ac.at.ju.configuration.Configuration;
import org.json.simple.JSONObject;

/**
 *
 * @author anindita
 */
public class HadoopJSONServer {
    private JSONObject inputDocument;
    private JSONObject jSONDataDocument;
    private String accountInfo;
    private String containerInfo;
    private String fileID;
    private String localFileName;
    private String fileExtension;
    StringConversion jsonObj=StringConversion.getInstance();

    DatabaseConnection objConnection=DatabaseConnection.getInstance();

    public void addInputDocument(String inputJsonString) {
        System.out.println("addInputDocument="+inputJsonString);
        jsonObj.mainReplaceStringtoObject(inputJsonString);
        this.inputDocument = jsonObj.jsonObj; 
    }

    public String getAccountInfo() {
        this.accountInfo=this.inputDocument.get("account").toString();
        System.out.println("this.accountInfo="+this.accountInfo);
        return accountInfo;
    }

    public String getContainerInfo() {
        this.containerInfo=this.inputDocument.get("container").toString();
        return containerInfo;
    }
    
    

    public void setJSONDataDocument(String inputJSONKey) {
        this.jSONDataDocument=(JSONObject)this.inputDocument.get(inputJSONKey);
        //return jSONDataDocument;
    }

    public String getFileID(JSONObject inputDataObj) {
        this.fileID=inputDataObj.get("id").toString();
        return fileID;
    }
    
   /* public String getLocalFileName(JSONObject inputDataObj) {
        String tempLocalFileName=inputDataObj.get("localfilepath").toString();
        if(tempLocalFileName.startsWith("sftp://"))
        {
            this.localFileName=Configuration.getConfig("BUFFER")+tempLocalFileName.substring(tempLocalFileName.lastIndexOf("/"));
        }
        else
        {
            this.localFileName=tempLocalFileName;
        }
        return localFileName;
    }*/
    
    /*public String getLocalFileName(JSONObject inputDataObj) {
        //String tempLocalFileName=inputDataObj.get("localfilepath").toString();
        
        //return tempLocalFileName;
        this.localFileName=inputDataObj.get("localfilepath").toString();
        return localFileName;
    }*/
    
    public String getFileExtension(JSONObject inputDataObj) {
        this.fileExtension=inputDataObj.get("fileextension").toString();
        return fileExtension;
    }
    
    public void databaseSetup(String inputJsonString)
    {
        addInputDocument(inputJsonString);         //conversion from jsonstring to jsonobject
        
        
        objConnection.setDatabaseInfo(getAccountInfo());
        objConnection.setTableInfo(getContainerInfo());
     }
    
    public JSONObject storageData(JSONObject inputjsonObj)
    {
       JSONObject temp=(JSONObject)inputjsonObj.clone();
       //temp.put("container", temp.get("account")+"_"+temp.get("operation"));
       temp.remove("id");
       return temp;
       
         
    }
    
    
    public String hadoopStorage(String inputJsonString)throws Exception
    {
        System.out.println("hadoopStorage="+inputJsonString);
        databaseSetup(inputJsonString);
        
        String output=new String();
        setJSONDataDocument("datagroups");
        objConnection.setId(getFileID(jSONDataDocument));
         
        //try
        {
       if(jSONDataDocument.containsKey("localfilepath"))
       {
         //objConnection.setLocalFileName(getLocalFileName(jSONDataDocument));
         //System.out.println("localfileName="+getLocalFileName(jSONDataDocument));
         objConnection.setFileExtension(localFileName.substring(localFileName.lastIndexOf(".")));
         objConnection.connection();
         output=objConnection.fileStore();
         objConnection.close();
       }
       else if(jSONDataDocument.containsKey("remotefiledata"))
       {
          objConnection.setFileExtension("."+getFileExtension(jSONDataDocument));
          String fileContent=storageData(jSONDataDocument).toString();
          objConnection.connection();
          String remoteString=jsonObj.remoteString;
          output=objConnection.remoteFileDataStore(fileContent,remoteString);
          objConnection.close();
       }
       else
       {
           objConnection.setFileExtension(".txt");
           String fileContent=storageData(jSONDataDocument).toString();
           objConnection.connection();
           output=objConnection.fileDataStore(fileContent);
           objConnection.close();
       }
        }
        /*catch(Exception e)
        {
            System.out.println("Exception from storage="+e);
        }*/
        
        return output;
       
    }
    
    public String hadoopRead(String inputJsonString) 
    {
        String output=new String();
        databaseSetup(inputJsonString);
        setJSONDataDocument("condition");
        objConnection.setId(getFileID(jSONDataDocument));
        //objConnection.setLocalFileName(getLocalFileName(jSONDataDocument));
        objConnection.setFileExtension("."+getFileExtension(jSONDataDocument));
        try
        {
       objConnection.connection();
       output=objConnection.fileRead();
       objConnection.close();
        }
        catch(Exception e)
        {
            System.out.println("Exception from read="+e);
        }
        System.out.println("HadoopJSONServer="+output);
        return output;
       
       
    }
    
    public String hadoopDelete(String inputJsonString) 
    {
        String output=new String();
        databaseSetup(inputJsonString);
        setJSONDataDocument("eliminate");
        objConnection.setId(getFileID(jSONDataDocument));
        objConnection.setFileExtension("."+getFileExtension(jSONDataDocument));
        try
        {
       objConnection.connection();
       output=objConnection.fileDelete();
       objConnection.close();
        }
        catch(Exception e)
        {
            System.out.println("Exception from delete="+e);
        }
        return output;
       
       
    }
    
 

}
