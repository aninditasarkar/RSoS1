package ac.at.ju.schemaDesign;

import ac.at.ju.storageUnit.CassandraConnector;
import ac.at.ju.informationCollections.DatasetJSONExtraction;
import ac.at.ju.informationCollections.InsertDataset;
import ac.at.ju.storageUnit.Configuration;
import java.util.HashMap;
import org.json.simple.JSONObject;

public class CassandraHandling {
    DatasetJSONExtraction databaseObj;
    JSONObject jsonobj;
    CassandraConnector obj;
    SchemaJSONExtraction schemaObj;

    public CassandraHandling() {
        this.jsonobj = new JSONObject();
        this.schemaObj = SchemaJSONExtraction.getInstance();
        this.databaseObj = DatasetJSONExtraction.getInstance();
        this.obj = new CassandraConnector();
    }

    public static CassandraHandling getInstance() {
        return new CassandraHandling();
    }

    public void setJSONObj(JSONObject jsonobj1) {
        this.jsonobj = jsonobj1;
    }

    public String getExtractedKeySpaceName() {
        this.schemaObj.setObj(this.jsonobj);
        return this.schemaObj.getAccount();
    }

    public String getExtractedColumnfamilyName() {
        this.schemaObj.setObj(this.jsonobj);
        return this.schemaObj.getContainer();
    }

    public HashMap getExtractedAttributeList() {
        this.schemaObj.setObj(this.jsonobj);
        return this.schemaObj.getAttribute();
    }

    public String getExtractedPrimaryKeyName() {
        this.schemaObj.setObj(this.jsonobj);
        return this.schemaObj.getPrimaryKeyName();
    }

    public String getExtractedPrimaryKeyType() {
        this.schemaObj.setObj(this.jsonobj);
        return this.schemaObj.getPrimaryKeyDataType();
    }

    public void cassandraConnection() {
        //this.obj.connect("localhost", 9042); ///for amazon
        this.obj.connect(Configuration.getConfig("IP"), Integer.parseInt(Configuration.getConfig("PORT")));   //for master
        this.obj.setKeyspaceName(getExtractedKeySpaceName());
        this.obj.setTableName(getExtractedColumnfamilyName());
    }

    public void schemaCreation() {
        this.obj.setKeyName(getExtractedPrimaryKeyName());
        this.obj.setKeyDataType(getExtractedPrimaryKeyType());
        this.obj.setAttributeSet(getExtractedAttributeList());
        this.obj.createKeyspace();
        this.obj.createtable();
    }

    public void dataInsert() {
        InsertDataset insertObj = InsertDataset.getInstance();
        insertObj.setCassandraObj(this.obj);
        insertObj.setJsonobj(this.jsonobj);
        insertObj.insertTableDataset();
    }

    public void close() {
        this.obj.close();
    }
}
