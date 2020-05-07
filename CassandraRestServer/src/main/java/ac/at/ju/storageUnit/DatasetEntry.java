package ac.at.ju.storageUnit;

//import ac.at.ju.cassandradbserver.LogGeneration;
import ac.at.ju.schemaDesign.CassandraHandling;
//import java.util.logging.Level;
//import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class DatasetEntry {
    private JSONObject jsonObject=new JSONObject();
    
    public void setJsonObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }
    

    
    public JSONObject jsonStringtoObject(String jsonString)
    {
        //LogGeneration.init();
        //Logger logger = Logger.getLogger(LogGeneration.class.getName());
        JSONObject requestObj = new JSONObject();
        try {
            requestObj = (JSONObject) new JSONParser().parse(jsonString);
           } catch (Exception e) {
            //logger.log(Level.SEVERE, null, e);
            //System.out.println("DatasetEntry Exception=" + e);
        }
        return requestObj;
    }
    
    public boolean dataStorage()
    {
        CassandraHandling schemaObj = CassandraHandling.getInstance();
        schemaObj.setJSONObj(this.jsonObject); //mandatory field
        schemaObj.cassandraConnection();
        schemaObj.schemaCreation();
        schemaObj.dataInsert();
        schemaObj.close();
        return true;
    }
    
    
    public String paramURIValue(JSONObject obj)
   {
       String account=obj.get("account").toString();
       String container=obj.get("container").toString();
       String uri=Configuration.getConfig("WEB_URI");
      //return obj.get("account").toString().concat(obj.get("container").toString());
      return uri+account+"&"+container;
   }
    
    
     public String mainReadJSONObject(String jsonString ) {
         //public String mainReadJSONObject(){
       //String jsonString = "{\n\t\"account\": \"U060\",\n\t\"container\":\"U010_ecg_123456789\",\n\t\"primarykeyname\":\"time\",\n\t\"primarykeytype\":\"varchar\",\n\t\"attribute\":\n\t\t\t[\n\t\t\t\t{\n\t\t\t\t\t\"name\":\"data\",\n\t\t\t\t\t\"type\":\"varchar\"\n\t\t\t\t},\n\t\t\t\t{\n\t\t\t\t\t\"name\":\"deviceid\",\n\t\t\t\t\t\"type\":\"varchar\"\n\t\t\t\t}\n\t\t\t],\n\t\"datagroups\":\n\t\t{\n\t\t\t\"deviceid\":\"1cbe2ad7-55c5-40a6-86da-9d3be32d37b9\",\n\t\t\t\"sensor_type\":\"ecg\",\n\t\t\t\"patient_name\":\"Sushanto\",\n\t\t\t\"patient_id\":\"P001\",\n\t\t\t\"dataSet\":\n\t\t\t\t[\n\t\t\t\t\t{\n\t\t\t\t\t\t\"data\":\"60\",\n\t\t\t\t\t\t\"time\":\"20160912_907865\"\n\t\t\t\t\t\t\n\t\t\t\t\t},\n\t\t\t\t\t{\n\t\t\t\t\t\t\"data\":\"86\",\n\t\t\t\t\t\t\"time\":\"20140309_234567\"\n\t\t\t\t\t}\n\t\t\t\t]\n\t\t}\n\n\n}\n";
    
        setJsonObject(jsonStringtoObject(jsonString));
        if(dataStorage())
        {
        //return paramURIValue(this.jsonObject);  //to view the storage location in cassandra
            return "output=Storage has been done in cassandra"; ///to show the action performed
        }
        else
        {
            return "unable to store!!!!!!!!!!";
        }
        
     }
   
    
    
    
   public static void main(String []p)
    {
        
       /* String inputData="{\n" +
"	\"account\": \"ghty\",\n" +
"	\"container\":\"U010_ecg_set\",\n" +
"	\"primarykeyname\":\"deviceid12\",\n" +
"	\"primarykeytype\":\"varchar\",\n" +
"	\"attribute\":\n" +
"			[\n" +
"				{\n" +
"					\"name\":\"data\",\n" +
"					\"type\":\"varchar\"\n" +
"				},\n" +
"				{\n" +
"					\"name\":\"time\",\n" +
"					\"type\":\"varchar\"\n" +
"				}\n" +
"			],\n" +
"	\"datagroups\":\n" +
"		{\n" +
"			\"deviceid12\":\"1cbe2ad7-55c5-40a6-86da-9d3be32d37b9\",\n" +
"			\"sensor_type\":\"ecg\",\n" +
"			\"patient_name\":\"Sushanto\",\n" +
"			\"patient_id\":\"P001\"\n" +
"			\n" +
"		}\n" +
"\n" +
"\n" +
"}";*/
        String inputData="{\n" +
"	\"account\": \"ghtyu1\",\n" +
"	\"container\":\"U010_ecg_set1\",\n" +
"	\"primarykeyname\":\"deviceid12\",\n" +
"	\"primarykeytype\":\"varchar\",\n" +
"	\"attribute\":\n" +
"			[\n" +
"				{\n" +
"					\"name\":\"data\",\n" +
"					\"type\":\"varchar\"\n" +
"				},\n" +
"				{\n" +
"					\"name\":\"time\",\n" +
"					\"type\":\"varchar\"\n" +
"				}\n" +
"			],\n" +
"	\"datagroups\":\n" +
"		{\n" +
"			\"sensor_type\":\"ecg\",\n" +
"			\"patient_name\":\"Sushanto\",\n" +
"			\"patient_id\":\"P001\"\n" +
"                       \"dataset\":\n"  +
"                           [\n"   +
"                               {\n"   +
"                                 \"deviceid12\":\"123\",\n"+                
"                                 \"data\":\"60\",\n"+
"                                  \"time\":\"20160912_907865\"\n"+
"                                },\n"+
"                                {\n"+
"                                  \"deviceid12\":\"1234\",\n"+                  
"                                 \"data\":\"86\",\n"+
"                                  \"time\":\"20140309_234567\"\n"+
"                                }\n"+
"                              ]\n"+
"			\n" +
"		}\n" +
"\n" +
"\n" +
"}";
       System.out.println("from main@@@@@"+ new DatasetEntry().mainReadJSONObject(inputData));
       }
}
   
   //public String readJSONObject(String jsonObject1) {
   /*public String mainReadJSONObject() {
      
        /*LogGeneration.init();
        Logger logger = Logger.getLogger(LogGeneration.class.getName());
        JSONObject requestObj = new JSONObject();
        try {
            requestObj = (JSONObject) new JSONParser().parse(this.jsonObject);
            setJsonObject(requestObj);
        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
            //System.out.println("DatasetEntry Exception=" + e);
        }CassandraHandling schemaObj = CassandraHandling.getInstance();
        schemaObj.setJSONObj(requestObj);
        schemaObj.cassandraConnection();
        schemaObj.schemaCreation();
        schemaObj.dataInsert();
        schemaObj.close();
        //return "{\"greetings\":\"success\"}";
    //}
   
   
    
}*/

