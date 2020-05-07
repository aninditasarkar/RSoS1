package ac.at.ju.storageUnit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import static java.lang.System.out;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
//import java.util.logging.Level;
//import java.util.logging.Logger;

public class CassandraConnector {
    private HashMap attributeSet;
    private Cluster cluster;
    private String keyDataType;
    private String keyName;
    private String keyspaceName;
    private HashMap rowValue;
    private Session session;
    private String tableName;
    private String condition;
    private String columnName;
    
    
        //Logger logger = Logger.getLogger(LogGeneration.class.getName());

    public CassandraConnector() {
        this.rowValue = new HashMap();
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    public void setKeyDataType(String keyDataType) {
        this.keyDataType = keyDataType;
    }

    public void setAttributeSet(HashMap attributeSet) {
        this.attributeSet = attributeSet;
    }

    public void setRowValue(HashMap rowValue) {
        this.rowValue = rowValue;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    //for master server
    public void connect( String node, int port)
   {
       //LogGeneration.init(); 
       try
       {
       
       this.cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
      final Metadata metadata = cluster.getMetadata();
      out.printf("Connected to cluster: %s\n", metadata.getClusterName());
      for (final Host host : metadata.getAllHosts())
      {
         out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
            host.getDatacenter(), host.getAddress(), host.getRack());
      }
      session = cluster.connect();
       }
       catch(Exception e)
       {
           //logger.log(Level.SEVERE, null, e);
           //System.out.println("exception ="+e);
       }
   }
    ///for amazon Server
    /*public void connect(String node, int port) {
        LogGeneration.init(); 
        //this.cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
        this.cluster = Cluster.builder().addContactPoint("localhost").build();
        System.out.printf("Connected to cluster: %s\n", new Object[]{this.cluster.getMetadata().getClusterName()});
        for (Host host : cluster.getMetadata().getAllHosts()) {
            
            Object value=new Object[]{host.getDatacenter(), host.getAddress(), host.getRack()};
            logger.log(Level.SEVERE, null, "Datacenter: %s; Host: %s; Rack: %s\n"+value.toString());
//System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", new Object[]{host.getDatacenter(), host.getAddress(), host.getRack()});
        }
        this.session = this.cluster.connect();
    }*/

    public boolean existKeyspace() {
        //for (Row row : this.session.execute("SELECT keyspace_name  FROM system.schema_keyspaces;")) {
        //LogGeneration.init();
        for (Row row : this.session.execute("SELECT keyspace_name  FROM system_schema.keyspaces;")) {    //for ubuntu uses cassandra 3.5
        //    logger.log(Level.SEVERE, null, "keyspace name=" + row.getString("keyspace_name") + "@@@@  " + this.keyspaceName + "####  " + this.tableName);
            //System.out.println("keyspace name=" + row.getString("keyspace_name") + "@@@@  " + this.keyspaceName + "####  " + this.tableName);
            if (row.getString("keyspace_name").equalsIgnoreCase(this.keyspaceName)) {
                return true;
            }
        }
        return false;
    }

    public void createKeyspace() {
        if (!existKeyspace()) {
            System.out.println("hello");
            this.session.execute("CREATE KEYSPACE " + this.keyspaceName + " WITH replication " + "= {'class':'SimpleStrategy', 'replication_factor':1};");
        }
    }

    public boolean existTable() 
    {
        
        //for (Row row : this.session.execute("SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name='" + this.keyspaceName.toLowerCase() + "';")) {  ///for cassandra 2.1, run in mac
            for (Row row : this.session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='" + this.keyspaceName.toLowerCase() + "';")) {               //for cassandra 3.5 run in ubuntu
        
            //System.out.println("table name=" + row.getString("columnfamily_name") + "@@@@  " + this.tableName);
            if (row.getString("table_name").equalsIgnoreCase(this.tableName)) {
                return true;
            }
        }
        return false;
    }

    public void createtable() {
        if (!existTable()) {
            String createtablequery = "CREATE TABLE " + this.keyspaceName + "." + this.tableName + " (" + this.keyName + " " + this.keyDataType + " PRIMARY KEY";
            Set set = this.attributeSet.entrySet();
              Iterator i = set.iterator();
                while(i.hasNext()) {
                    Map.Entry me = (Map.Entry)i.next();
            //for (Entry me : this.attributeSet.entrySet().iterator()) {
                createtablequery = createtablequery + "," + me.getKey().toString() + " " + me.getValue().toString();
           }
            this.session.execute(createtablequery + ");");
        }
    }

    public void insertData() {
        System.out.println("insertData");
        String columnRow = new String();
        String columnValue = new String();
        Set set = this.rowValue.entrySet();
              Iterator i = set.iterator();
                while(i.hasNext()) {
                    Map.Entry me = (Map.Entry)i.next();
          //for (Entry me : this.rowValue.entrySet()) {
            columnRow = columnRow + "," + me.getKey().toString();
            System.out.println("columnRow=====" + columnRow);
            columnValue = columnValue + "," + me.getValue().toString();
            System.out.println("columnValue=====" + columnValue);
        }
        System.out.println("columnRow%%%%%=" + columnRow + " columnValue===" + columnValue);
        this.session.execute("INSERT INTO " + this.keyspaceName + "." + this.tableName + " (" + columnRow.substring(1) + ")" + " VALUES " + "(" + columnValue.substring(1) + ");");
    }

    public List<Row> viewAllData()
   {
       String viewAllQuery="SELECT * FROM "+keyspaceName+"."+tableName+";";
       List<Row> value=session.execute(viewAllQuery).all();
       return value;
       /*String rowValue="";
       for(Row row : results)
       {
           rowValue=row.getString(keyName);
           for(int i=0;i<columnName.size();i++)
           {
               rowValue+=row.getString(columnName.get(i));
           }
       }
       System.out.println("row is view!!!!!!"+rowValue);*/
   }
    
    public List<Row> viewConditionData()
   {
       String rowValue="";
       String query="SELECT * FROM "+keyspaceName+"."+tableName+
               " WHERE "+condition+" ALLOW FILTERING;";
       List<Row> results=session.execute(query).all();
      return results;
   }
    
    public List<Row> viewConditionColumnData()
   {
       String rowValue="";
       String query="SELECT "+columnName+" FROM "+keyspaceName+"."+tableName+
               " WHERE "+condition+" ALLOW FILTERING;";
       List<Row> results=session.execute(query).all();
       System.out.println("from viewConditionColumnData="+results);
      return results;
   }
    
    public long getRowNumber()
    {
        long rowValue=0;
        String query="SELECT COUNT(1) From "+keyspaceName+"."+tableName+";";
        List<Row> results=session.execute(query).all();
        for(Row row : results)
       {
           rowValue=row.getLong("count");
       }
        System.out.println("from getRowNumber="+rowValue);
      return rowValue;
    }
    
    public String getPartitionKey()
    {
        String rowValue="";
       String query="select column_name from system_schema.columns "
               + "where keyspace_name='"+keyspaceName+"' and "
               + "table_name='"+tableName+"' and kind='partition_key' allow filtering;";
      System.out.println("from getPartitionKey="+tableName);
       List<Row> results=session.execute(query).all();
       for(Row row : results)
       {
           rowValue=row.getString("column_name");
       }
      return rowValue;
    }
    
    public String deleteRow()
    {
        String output=null;
        if(viewConditionData().size()==0)
        {
            output="the requested row object is not exist ";
        }
        else
        {
        String query="delete from "+keyspaceName+"."+tableName+" where "+condition+";";
        this.session.execute(query);
        output="the requested row object is deleted";
        }
        return output;
    }
    
    public void deleteTable()
    {
        String query="DROP TABLE "+keyspaceName+"."+tableName+";";
        session.execute(query);
    }

    public Session getSession() {
        return this.session;
    }

    public void close() {
        try {
            this.cluster.close();
        } catch (Exception e) {
            System.out.println("CassandraConnector close exception=" + e);
        }

    }
}
