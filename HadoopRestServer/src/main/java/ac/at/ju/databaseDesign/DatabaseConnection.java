 /*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.databaseDesign;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import static java.lang.System.out;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
//import javax.ws.rs.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
//import org.json.simple.JSONObject;
/**
 *
 * @author anindita
 */
public class DatabaseConnection {
    
private Configuration conf;
private FileSystem hdfs;
private String localFileName;
private String databaseInfo;
private String tableInfo;
private String fileID;
private Path hadoopDirectoryPath;
private Path hadoopContainerPath;
private Path hadoopAccountPath;
private String fileExtension;

public static DatabaseConnection getInstance()
{
    return new DatabaseConnection();
}

    public void setLocalFileName(String localFileName) {
        this.localFileName = localFileName;
    }

    
    public void setId(String fileID) {
        this.fileID = fileID;
    }

    public void setDatabaseInfo(String databaseInfo) {
        this.databaseInfo = databaseInfo;
    }

    public void setTableInfo(String tableInfo) {
        this.tableInfo = tableInfo;
    }

public void setFileExtension(String fileExtension)
{
    this.fileExtension=fileExtension;
}

    public void setHadoopDirectoryPath() {
        this.hadoopDirectoryPath = new Path("hdfs://10.10.0.15:9000/"+databaseInfo+"/"+tableInfo+"/"+fileID+fileExtension);
    
    }

    public void setHadoopAccountPath() {
        this.hadoopAccountPath = new Path("hdfs://10.10.0.15:9000/"+databaseInfo);;
    }
    
    public void setHadoopContainerPath() {
        this.hadoopContainerPath = new Path("hdfs://10.10.0.15:9000/"+databaseInfo+"/"+tableInfo);;
    }
    
    
public int getContainerLength() throws FileNotFoundException, IOException {
    List<String> fileList = new ArrayList<String>();
    FileStatus[] fileStatus = hdfs.listStatus(hadoopContainerPath);
    for (FileStatus fileStat : fileStatus) {
        fileList.add(fileStat.getPath().getName());
    }
    return fileList.size();
}

public void connection()throws Exception
{
    conf=new Configuration();
    //need for more advanced version to upload hadoop information
    conf.addResource(new Path("/home/anindita/Documents/Tool/hadoop-2.6.0/etc/hadoop/core-site.xml"));
    conf.addResource(new Path("/home/anindita/Documents/Tool/hadoop-2.6.0/etc/hadoop/hdfs-site.xml"));
    ////end
    
    //hdfs=FileSystem.get(conf);
     hdfs = FileSystem.get(new URI("hdfs://10.10.0.15:9000/"), conf);
    
    
    setHadoopAccountPath();
    setHadoopContainerPath();
    setHadoopDirectoryPath();
    
    }

public void createAccountDirectoryPath()throws Exception
    {
     hdfs.mkdirs(this.hadoopAccountPath);   //create directory
     
    }

public void createContainerDirectoryPath()throws Exception
    {
        hdfs.mkdirs(this.hadoopContainerPath);   //create directory
    }


///Store file contents from file mentioned the location of file in json object
public String fileStore()throws Exception
{
    if (hdfs.exists(this.hadoopDirectoryPath)) {
        hdfs.close();
        return "output= "+fileID+" already exists in "+hadoopDirectoryPath;
         }
    else
    {
    if(!hdfs.exists(this.hadoopAccountPath))
            {
        createAccountDirectoryPath();
    }
    else if(!hdfs.exists(this.hadoopContainerPath))
    {
        createContainerDirectoryPath(); 
    }
    
    //hdfs.copyFromLocalFile(new Path(localFileName),hadoopDirectoryPath);
    InputStream in = new BufferedInputStream(new FileInputStream(
        new File(this.localFileName)));
    FSDataOutputStream out = hdfs.create(this.hadoopDirectoryPath); //create file in directory

    byte[] b = new byte[1024];
    int numBytes = 0;
    while ((numBytes = in.read(b)) > 0) {
        out.write(b, 0, numBytes);
    }
    in.close();
    out.close();
    hdfs.close();
   return "output=data storage done in hadoop at "+hadoopDirectoryPath;
   }
}

///For storing file contents directly from json object
public String fileDataStore(String inputData)throws IOException
{
    System.out.println("fileDataStore.inputData="+inputData);
    /*conf=new Configuration();
    //need for more advanced version to upload hadoop information
    conf.addResource(new Path("/home/anindita/Documents/Tool/hadoop-2.6.0/etc/hadoop/core-site.xml"));
    conf.addResource(new Path("/home/anindita/Documents/Tool/hadoop-2.6.0/etc/hadoop/hdfs-site.xml"));*/
        
    if (hdfs.exists(this.hadoopDirectoryPath)) {
        hdfs.close();
        return "output= "+fileID+" already exists in "+hadoopDirectoryPath;
        
         }
    else
    {   
    FSDataOutputStream fin = hdfs.create(hadoopDirectoryPath);
    fin.write(inputData.getBytes());
   //fin.writeUTF(inputData);
    fin.close();
    hdfs.close();
   return "output=data storage done in hadoop at******** "+hadoopDirectoryPath;
    }
   
}

///For storing file contents directly from json object
public String remoteFileDataStore(String inputData, String remoteString)throws Exception
{
    System.out.println("remoteFileDataStore.inputData="+inputData);
    /*conf=new Configuration();
    //need for more advanced version to upload hadoop information
    conf.addResource(new Path("/home/anindita/Documents/Tool/hadoop-2.6.0/etc/hadoop/core-site.xml"));
    conf.addResource(new Path("/home/anindita/Documents/Tool/hadoop-2.6.0/etc/hadoop/hdfs-site.xml"));*/
        
    if (hdfs.exists(this.hadoopDirectoryPath)) {
        hdfs.close();
        return "output= "+fileID+" already exists in "+hadoopDirectoryPath;
         }
    else
    {
        //JSONObject fileContentObj=StringConversionForRemoteFileData.getInstance().jsonStringtoObject(inputData);
        FSDataOutputStream hdfsout = hdfs.create(hadoopDirectoryPath);
        
        JSONObject fileContentObj=(JSONObject) new JSONParser().parse(inputData);
        String fileExtension=fileContentObj.get("fileextension").toString();
        //String fileContent=fileContentObj.get("remotefiledata").toString();
        //int index=remoteString.indexOf("\"remotefiledata\":");
        int index1="\"remotefiledata\":".length()+1;
        int index2=remoteString.lastIndexOf("\",");
        String remoteData=remoteString.substring(index1,index2);
        
        //for image data file
        if(fileExtension.equalsIgnoreCase("png")||fileExtension.equalsIgnoreCase("jpg")||fileExtension.equalsIgnoreCase("gif"))
        {
           byte[] imageByteArray = Base64.decodeBase64(remoteData); 
           hdfsout.write(imageByteArray);
        }
        else   //for text data file
        {
           hdfsout.write(remoteData.getBytes());
    
        }
        System.out.println("DatabaseConnection.remoteFileDataStore.remoteData="+remoteData);
        
    
    //fin.write(fileContent.getBytes());
    //fin.writeUTF(inputData);
    hdfsout.close();
    hdfs.close();
   return "output=data storage done in hadoop at******** "+hadoopDirectoryPath;
    }
   
}




public String fileRead()throws IOException
{
    String output=null;
    if (!hdfs.exists(this.hadoopDirectoryPath)) {
        hdfs.close();
         output= "output= "+fileID+" does not exists in "+hadoopDirectoryPath;
        
         }
    else
    {
    FSDataInputStream hdfsin = hdfs.open(this.hadoopDirectoryPath);
    
    
    
    //hdfs.copyToLocalFile(hadoopDirectoryPath, new Path(this.localFileName));
    if(fileExtension.equalsIgnoreCase(".png")||fileExtension.equalsIgnoreCase(".jpg")||fileExtension.equalsIgnoreCase(".gif"))
        {
            //int fileLength=(int)hdfs.getContentSummary(this.hadoopDirectoryPath).getLength();
            //byte imageData[] = new byte[fileLength];
            //byte imageData[] =FileUtils.readFileToByteArray(hdfs);
            //hdfsin.read(imageData,0,fileLength);
            
            //output = Base64.encodeBase64String(imageData);
            byte[] fileContent = IOUtils.toByteArray(hdfsin);
            hdfsin.close();
            output = Base64.encodeBase64String(fileContent);
            //System.out.println("DatabaseConnection.outputlength.imagefileRead="+output.length()+" bytelength="+imageData.length+" byteContent="+imageData[125761]+imageData[125762]+imageData[125763]);
            
            //byte[] decodedBytes = Base64.decodeBase64(output);
            //FileUtils.writeByteArrayToFile(new File("/home/anindita/Desktop/BenchmarkGain1.png"), decodedBytes);
            
            
            
            /*FileOutputStream imageOutFile = new FileOutputStream(new File("/home/anindita/Desktop/BenchmarkGain2.png/"));
            StringBuilder sb=new StringBuilder();
            String str="0";
            int c;
            while((c=hdfsin.read())!=-1)
            {
            //fos.write(c);
                //sb.append(c);
                str=str.concat(Integer.toString(c));
            }
           // imageOutFile.write(Integer.parseInt(sb.toString()));
            imageOutFile.write(Integer.valueOf(str.trim()));
            output=str;
            //output =sb.toString();*/
        }
    else
    {
        BufferedReader br = new BufferedReader(new InputStreamReader(hdfsin));
        StringBuilder sb=new StringBuilder();
        String read=null;

        while((read=br.readLine()) != null) {
            sb.append(read);
        }
        br.close();
        output=sb.toString();
    }
    
    hdfs.close();
    
    }
    System.out.println("DatabaseConnection="+output);
    return output;
    
}


public String fileDelete() throws IOException
{
    
    if (hdfs.exists(hadoopDirectoryPath)) {
        hdfs.delete(hadoopDirectoryPath, true);
        if(getContainerLength()==0)
        {
        hdfs.delete(hadoopContainerPath, true);
        hdfs.close();
        return "output=data deletion done of: "+fileID+fileExtension+" with container "+tableInfo;
        }
        else
        {
            hdfs.close();
        return "output=data deletion done of: "+fileID+fileExtension;
        }
    }
    else
    {
        hdfs.close();
        return "output= "+fileID+fileExtension+" does not exists";
         }
    
 }

public void close()throws IOException
{
    hdfs.close();
}

/*public static void main(String []p)throws Exception
{
    /*System.setProperty("http.proxyHost", "192.168.250.21");
        System.setProperty("http.proxyPort", "3128");
        System.setProperty("https.proxyHost", "192.168.250.21");
        System.setProperty("https.proxyPort", "3128");
        System.setProperty("ftp.proxyHost", "192.168.250.21");
        System.setProperty("ftp.proxyPort", "3128");
        */
    /*DatabaseConnection obj=new DatabaseConnection();
    obj.connection();
    obj.close();
}*/


}


//not needed
/*public String fileRead()throws IOException
{
    String output=null;
    if (!hdfs.exists(this.hadoopDirectoryPath)) {
        hdfs.close();
         output= "output= "+fileID+" does not exists in "+hadoopDirectoryPath;
        
         }
    else
    {
    FSDataInputStream hdfsin = hdfs.open(this.hadoopDirectoryPath);
//hdfs.copyToLocalFile(hadoopDirectoryPath, new Path(this.localFileName));
System.out.println("DatabaseConnection.fileExtension="+fileExtension);
//if(fileExtension.equalsIgnoreCase(".png")||fileExtension.equalsIgnoreCase(".jpg")||fileExtension.equalsIgnoreCase(".gif"))
/*{
                        int fileLength=(int)hdfs.getContentSummary(this.hadoopDirectoryPath).getLength();
			System.out.println("DatabaseConnection.fileLength="+fileLength);
                        //byte imageData[] = new byte[fileLength];
                        //int numByte=0;
                        byte imageData[] = new byte[1024];
                        //while(numByte>=fileLength)
                            /*for(int numByte=0;numByte<=fileLength;numByte+=1024)
                        {
                            System.out.println("numByte="+numByte+" fileLength="+fileLength);
			hdfsin.read(imageData, 0,numByte);*/
                            /*int c;
                        while((c=hdfsin.read(imageData))!=-1)
                        {
			
			/*
			 * Converting Image byte array into Base64 String 
			 */
			/*String imageDataString = Base64.encodeBase64URLSafeString(imageData);
                        byte[] imageByteArray = Base64.decodeBase64(imageDataString); 
                        FileOutputStream imageOutFile = new FileOutputStream(new File(this.localFileName));
           imageOutFile.write(imageByteArray);
          // numByte+=1024;
           
                        }
                        output="output=file downloaded in: "+this.localFileName;
                                }*/
       /* {
                                            FileOutputStream fos=new FileOutputStream(new File(this.localFileName));
int c;
while((c=hdfsin.read())!=-1)
{
fos.write(c);
}
output="output=file downloaded in: "+this.localFileName;
                                        }

/*else
{

    BufferedWriter bw = new BufferedWriter(new FileWriter(new File(this.localFileName)));
    BufferedReader br = new BufferedReader(new InputStreamReader(hdfsin));
        String read;

        while((read=br.readLine()) != null) {
            //System.out.println(read);
            //sb.append(read);
             bw.write(read);
        }
       
    
    //hdfs.close();
    bw.close();
    br.close();
    output="output=file downloaded in: "+this.localFileName;
}*/
    /*hdfsin.close();
    hdfs.close();
    
    }
    return output;
    
}*/
