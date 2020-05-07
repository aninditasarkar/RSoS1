package ac.at.ju.informationCollections;

import ac.at.ju.schemaDesign.SchemaJSONExtraction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class DatasetJSONExtraction {
    int attributeValueNumber;
    JSONObject jsonobj;
    SchemaJSONExtraction schemaObj;
    HashMap<String, LinkedList<String>> valueSet;

    public DatasetJSONExtraction() {
        this.valueSet = new HashMap();
        this.attributeValueNumber = 1; //set value from 0 to 1
        this.schemaObj = SchemaJSONExtraction.getInstance();
    }

    public static DatasetJSONExtraction getInstance() {
        return new DatasetJSONExtraction();
    }

    public void setDatabaseJSONObj(JSONObject objMain1) {
        this.jsonobj = objMain1;
    }

    public JSONObject getJsonobj() {
        return this.jsonobj;
    }

    public int getAttributeValueNumber() {
        return this.attributeValueNumber;
    }

    public void setAttributeValueNumber(int attributeValueNumber1) {
        this.attributeValueNumber = attributeValueNumber1;
    }

    public LinkedList<String> columnValueExtractionArray(JSONArray datasetArray, String columnName) {
        LinkedList<String> columnValue = new LinkedList();
        Iterator<JSONObject> iterator = datasetArray.iterator();
        this.attributeValueNumber = 0; //change value from 0 to 1
        while (iterator.hasNext()) {
            columnValue.add("'" + ((JSONObject) iterator.next()).get(columnName).toString() + "'");
            this.attributeValueNumber++;
        }
        setAttributeValueNumber(this.attributeValueNumber);
        return columnValue;
    }

    public LinkedList<String> columnValueExtraction(JSONObject datagroupsObj, String columnName) {
        LinkedList<String> columnValue = new LinkedList();
        try {
            columnValue.add("'" + datagroupsObj.get(columnName).toString() + "'");
            System.out.println("DataValue___"+columnName+"   ----->"+columnValue);
            return columnValue;
        } catch (Exception e) {
            columnValue=columnValueExtractionArray((JSONArray) datagroupsObj.get("dataset"), columnName);
            System.out.println("DatasetJSONExtraction exception=" + e);
            return columnValue;
        }
    }

    public LinkedList<String> getColumnValue(String columnName, String columnDataType) {
        return columnValueExtraction((JSONObject) this.jsonobj.get("datagroups"), columnName);  ///
       
    }

    public HashMap getValueSet() {
        this.schemaObj.setObj(this.jsonobj);
        HashMap attributeList = this.schemaObj.getAttribute();
        attributeList.put(this.schemaObj.getPrimaryKeyName(), this.schemaObj.getPrimaryKeyDataType());
       
        Set set = attributeList.entrySet();
              Iterator i = set.iterator();
                while(i.hasNext()) {
                    Map.Entry me = (Map.Entry)i.next();
          
        //for (Entry me : attributeList.entrySet()) {
            String columnName = me.getKey().toString();
            String columnDataType = me.getValue().toString();
            System.out.println("hashmap  ======" + columnName);
            this.valueSet.put(columnName, getColumnValue(columnName, columnDataType));
        }
        return this.valueSet;
    }
}
