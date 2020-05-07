package ac.at.ju.informationCollections;

import ac.at.ju.storageUnit.CassandraConnector;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import org.json.simple.JSONObject;

public class InsertDataset {
    int attributeNumber;
    CassandraConnector cassandraObj;
    DatasetJSONExtraction databaseObj;
    JSONObject jsonobj;
    HashMap<String, String> rowValue;
    HashMap<String, LinkedList<String>> valueSet;

    public InsertDataset() {
        this.jsonobj = new JSONObject();
        this.databaseObj = DatasetJSONExtraction.getInstance();
        this.rowValue = new HashMap();
        this.attributeNumber =0;
        this.valueSet = new HashMap();
    }

    public static InsertDataset getInstance() {
        return new InsertDataset();
    }

    public void setCassandraObj(CassandraConnector cassandraObj1) {
        this.cassandraObj = cassandraObj1;
    }

    public void setAttributeNumber(int attributeNumber) {
        this.attributeNumber = attributeNumber;
    }

    public void setJsonobj(JSONObject jsonobj1) {
        this.jsonobj = jsonobj1;
    }

    public void setValueSet() {
        this.databaseObj.setDatabaseJSONObj(this.jsonobj);
        this.valueSet = this.databaseObj.getValueSet();
    }

    public HashMap tupleDataRetrieval(int position) {
        HashMap rowValue1 = new HashMap();
        for (Entry me : this.valueSet.entrySet()) {
            LinkedList<String> columnValue = (LinkedList) me.getValue();
            if (columnValue.size() == 1) {
                rowValue1.put(me.getKey().toString(), columnValue.get(0));
            } else {
                rowValue1.put(me.getKey().toString(), columnValue.get(position));
            }
        }
        return rowValue1;
    }

    public void insertTableDataset() {
        setValueSet();
        System.out.println("insertTableDataset="+this.databaseObj.getAttributeValueNumber());
        for (int k = 0; k < this.databaseObj.getAttributeValueNumber(); k++) {
            System.out.println("insertTableDataset(k)");
            this.cassandraObj.setRowValue(tupleDataRetrieval(k));
            this.cassandraObj.insertData();
        }
    }
}
