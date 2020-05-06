/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.demo;

import ac.at.ju.databaseDesign.HadoopJSONServer;
import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import javax.imageio.ImageIO;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
//import org.apache.commons.io.FileUtils;
import org.jboss.netty.handler.codec.base64.Base64Decoder;
import sun.misc.BASE64Decoder;



/**
 *
 * @author anindita
 */
public class TestDocumentView {
    
    public void localFileDataWrite(String input)throws Exception
    {
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File("/home/anindita/Desktop/a.txt")));
        bw.write(input);
        bw.close();
        System.out.println("file is generated="+input);
    }
    
     public void localImageDataWrite1(String input)throws Exception
           
    {
        //try
        {
        /*byte[] imageByteArray1 = Base64.decodeBase64(input);
        System.out.println("localImageDataWrite1 imageByteArray1.length="+imageByteArray1.length);
        byte[] imageByteArray= new BASE64Decoder().decodeBuffer(input);
        System.out.println("localImageDataWrite1 imageByteArray.length="+imageByteArray.length+" content="+imageByteArray[125763]+imageByteArray[125764]+imageByteArray[125765]);
        //byte[] imageByteArray1= Arrays.copyOfRange(imageByteArray, 0, 125764);
        //System.out.println("localImageDataWrite1 imageByteArray.length="+imageByteArray1.length);
        ByteArrayInputStream bis=new ByteArrayInputStream(imageByteArray1);
        bis.close();
        BufferedImage image = ImageIO.read(bis);
        
         ImageIO.write(image, "png", new File("/home/anindita/Desktop/BenchmarkGain2.png"));*/
        byte[] decodedBytes = Base64.decodeBase64(input);
        FileUtils.writeByteArrayToFile(new File("/home/anindita/Desktop/BenchmarkGain2.png"), decodedBytes);
        
        }
       /* catch(Exception e)
        {
            System.out.println("Exception="+e);
        }*/
        
        
    }
    
    
    public static void main(String []p)throws Exception
    {
        String json="{\n" +
"   \"account\":\"U104\",\n" +
"   \"container\":\"ecg\",\n" +
"   \"condition\":{\n" +
"      \"id\":\"BenchmarkGain1100\",\n" +
"      \"fileextension\":\"png\",\n"+
//"      \"localfilepath\":\"/home/anindita/Desktop/BenchmarkGain2.png\"\n"+  //unable to download the file in this location

"   }\n" +
"}";
       HadoopJSONServer obj=new HadoopJSONServer();
       String output=obj.hadoopRead(json);
       System.out.println("TestDocumentView="+output);
       System.out.println("TestDocumentView.outputlength="+output.length());
       //new TestDocumentView().localFileDataWrite(output);
       //Thread.sleep(60000);
      new TestDocumentView().localImageDataWrite1(output);
    }
}
