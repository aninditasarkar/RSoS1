package ac.at.ju.demo;


import ac.at.ju.databaseDesign.DatabaseConnection;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author anindita
 */
public class TestHadoop {
    public static List<String> getAllFilePath(Path filePath, FileSystem fs) throws FileNotFoundException, IOException {
    List<String> fileList = new ArrayList<String>();
    
    FileStatus[] fileStatus = fs.listStatus(filePath);
    for (FileStatus fileStat : fileStatus) {
        
        /*if (fileStat.isDir()) {
            fileList.addAll(getAllFilePath(fileStat.getPath(), fs));
            System.out.println("test12");
        } else {
            String fileName = fileStat.getPath().toString(); 
        fileList.add(fileName.substring(fileName.lastIndexOf("/") + 1));
        System.out.println("test1234");
        }*/
         fileList.add(fileStat.getPath().getName());
    }
    return fileList;
}

public static void main(String[] args) throws Exception 
  {
      System.setProperty("http.proxyHost", "172.16.15.8");
    
	System.setProperty("http.proxyPort", "8080");
	System.setProperty("https.proxyHost", "172.16.15.8");
	System.setProperty("https.proxyPort", "8080");
	System.setProperty("ftp.proxyHost", "172.16.15.8");
	System.setProperty("ftp.proxyPort", "8080");
        
      DatabaseConnection connectionObj=DatabaseConnection.getInstance();
      connectionObj.connection();
      connectionObj.setId("test12");
      
      
    Configuration conf = new Configuration();
    
    conf.addResource(new Path("/home/anindita/Documents/Tool/hadoop-2.6.0/etc/hadoop/core-site.xml"));
   //conf.addResource(new Path("/home/anindita/Documents/Tool/hadoop-2.6.0/etc/hadoop/hdfs-site.xml"));
   System.out.println("Connecting to -- "+conf.get("fs.defaultFS"));
   conf.set("fs.defaultFS", "hdfs://localhost:9000");
    //FileSystem fs = FileSystem.get(URI.create("hdfs://localhost"),conf); //not work, here 
                  //hadoop installation have a role, in my case hostname is ITRA rather than localhost.
    
    FileSystem fs = FileSystem.get(conf);
     //FileSystem fs = FileSystem.get(URI.create("/home/anindita/Documents/NOTICE12.txt"), conf);
//OutputStream out = fs.create(new Path("file:///"+conf.get("fs.defaultFS")+"/home/anindita/Documents/NOTICE12.txt"));
     System.out.println("working directory="+fs.getHomeDirectory());
    System.out.println("working directory="+fs.getWorkingDirectory());
    
   
    //Path destFilenamePath = new Path("/home/anindita/Documents/Tool/hadoop-2.6.0/input/NOTICE12.png");  
//Path destFilenamePath = new Path("hdfs://localhost:9000/home/anindita/Documents/NOTICE12.txt");
Path destFilenamePath = new Path(fs.getHomeDirectory()+"/hadoopinput12/ab/NOTICE1234.png");    
//try {
    if (fs.exists(new Path(fs.getHomeDirectory()+"/hadoopinput12/ab"))) {
        fs.delete(new Path(fs.getHomeDirectory()+"/hadoopinput12/ab"), true);
    }

    /*FSDataOutputStream fin = fs.create(filenamePath);
    fin.writeUTF("hello");
    fin.close();*/
    System.out.println("before file exists="+fs.exists(destFilenamePath));
    //fs.create(destFilenamePath,true);
    System.out.println("after file exists="+fs.exists(destFilenamePath));
    //fs.copyFromLocalFile(new Path("/home/anindita/Desktop/test.png"),destFilenamePath);
    //fs.copyToLocalFile(destFilenamePath,
                //  new Path("/home/anindita/Documents/test12.png"));
    List<String> fileStatusTemp=new TestHadoop().getAllFilePath(new Path(fs.getHomeDirectory()+"/hadoopinput12/ab"),fs);   
    System.out.println("file exists="+fileStatusTemp.size());
    for(int i=0;i<fileStatusTemp.size();i++)
    {
                      System.out.println("file exists="+fileStatusTemp.get(i));
    }
    System.out.println("exist 1234");
/*}
    catch(Exception e)
    {
        System.out.println("exception="+e);
    }*/
    
 /*Configuration config = new Configuration();
   config.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
   config.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

   config.set("fs.hdfs.impl", 
            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
       config.set("fs.file.impl",
            org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
  FileSystem dfs = FileSystem.get(config);
  String dirName = "TestDirectory";
  System.out.println(dfs.getWorkingDirectory() +" this is from /n/n");*/
  }
 
    
}
