package ac.at.ju.schemaDesign;

import java.util.HashMap;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class SchemaJSONExtraction {
    private JSONObject jsonobj;

    public static SchemaJSONExtraction getInstance() {
        return new SchemaJSONExtraction();
    }

    public void setObj(JSONObject obj1) {
        this.jsonobj = obj1;
    }

    public JSONObject getJsonobj() {
        return this.jsonobj;
    }

    public String getAccount() {
        return this.jsonobj.get("account").toString();
    }

    public String getPrimaryKeyName() {
        return getJsonobj().get("primarykeyname").toString();
    }

    public String getPrimaryKeyDataType() {
        return getJsonobj().get("primarykeytype").toString();
    }

    public String getContainer() {
        return getJsonobj().get("container").toString();
    }

    public HashMap getAttribute() {
        HashMap<String, String> attribute = new HashMap();
        Iterator<JSONObject> iterator = ((JSONArray) this.jsonobj.get("attribute")).iterator();
        while (iterator.hasNext()) {
            JSONObject attributeObj = (JSONObject) iterator.next();
            attribute.put(attributeObj.get("name").toString(), attributeObj.get("type").toString());
        }
        return attribute;
    }
}
