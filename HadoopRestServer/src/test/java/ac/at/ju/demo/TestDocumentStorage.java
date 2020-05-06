/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.demo;

import ac.at.ju.databaseDesign.HadoopJSONServer;
import static ac.at.ju.test.ImageFileWrite.encodeImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import static java.lang.System.in;
import org.apache.commons.codec.binary.Base64;

/**
 *
 * @author anindita
 */
public class TestDocumentStorage {
    
    //this method will be called for text file
    public String remoteFileDataContent()throws Exception
    {
        //InputStream inputStream = new FileInputStream("/home/anindita/Desktop/BenchmarkGain.png");
        InputStream inputStream = new FileInputStream("/home/anindita/Desktop/a.txt");
        StringBuilder sb=new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String read;

        while((read=br.readLine()) != null) {
            //System.out.println(read);
            sb.append(read);   
        }
        inputStream.close();
         br.close(); 
         System.out.println("remotefile="+sb.toString());
         return sb.toString();
    }
    
    //this method will be called for image data file
    public String remoteImageDataContent()throws Exception
    {
        File file=new File("/home/anindita/Desktop/BenchmarkGain.jpg");
        FileInputStream imageInFile = new FileInputStream(file);
			byte imageData[] = new byte[(int)file.length()];
			imageInFile.read(imageData);
			
			/*
			 * Converting Image byte array into Base64 String 
			 */
			String imageDataString = Base64.encodeBase64URLSafeString(imageData);
                        return imageDataString;
    }
    
    
    
    
    public static void main(String []p)throws Exception
    {
      //Only for the remote file storage follow some instruction
        //instruction 1: data contents pass as string
        //instruction 2: after placing remotefiledata content place id
        //***********the most important thing, in datagroups should follow the mentioned order of the attribute otherwise code will not run 
        //***************the attribute order under data groups of HDFSRESTServer and SchemaOBDBSupport is different
        
        
        
        
        String jsonRemoteFile="{\n" +
"   \"account\":\"i198\",\n" +             //U098
"   \"container\":\"ecg\",\n" +
"   \"datagroups\":{\n" +
//"      \"fileextension\":\"txt\"\n"+
"      \"fileextension\":\"jpg\"\n"+                
//"       \"remotefiledata\":\""+new TestDocumentStorage().remoteFileDataContent()+"\",\n" +   //for text file content
"       \"remotefiledata\":\""+new TestDocumentStorage().remoteImageDataContent()+"\",\n" +   //for image file content   
"      \"id\":\"BenchmarkGain41\",\n" +
                
"   }\n" +
"}";
        
        

    String jsonLocalFile="{\n" +
"   \"account\":\"i198\",\n" +             //U098
"   \"container\":\"ecg\",\n" +
"   \"datagroups\":{\n" +
"      \"id\":\"BenchmarkGain39\",\n" +
"      \"localfilepath\":\"/home/anindita/Desktop/BenchmarkGain.png\",\n" +
//"      \"localfilepath\":\"sftp://dsg@10.10.0.8;kartick/Users/dsg/Documents/HDFSDataset/image1.png\",\n" +
"   }\n" +
"}";
    
    String jsonData="{\n" +
"   \"account\":\"i198\",\n" +             //U098
"   \"container\":\"ecg\",\n" +
"   \"datagroups\":{\n" +
"      \"id\":\"BenchmarkGain39\",\n" +
"      \"name\":\"hello\",\n" +

"   }\n" +
"}";
    
       HadoopJSONServer obj=new HadoopJSONServer();
       System.out.println(obj.hadoopStorage(jsonRemoteFile));
       //String str="/home/anindita/Desktop/test.png";
       //System.out.println("output="+str.substring(str.indexOf(".")));
     
    }
}
