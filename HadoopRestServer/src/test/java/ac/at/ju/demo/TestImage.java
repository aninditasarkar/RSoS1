/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.demo;

import java.io.File;
import java.io.FileInputStream;
//import java.util.Base64;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author anindita
 */
public class TestImage {
    public static void main(String []p)throws Exception
    {
        //byte[] fileContent = FileUtils.readFileToByteArray(new File("/home/anindita/Desktop/BenchmarkGain.png"));
        byte[] fileContent = IOUtils.toByteArray(new FileInputStream(new File("/home/anindita/Desktop/BenchmarkGain.png") ));
//String encodedString = Base64.getEncoder().encodeToString(fileContent);
String encodedString = Base64.encodeBase64String(fileContent);
//byte[] decodedBytes = Base64.getDecoder().decode(encodedString);
byte[] decodedBytes = Base64.decodeBase64(encodedString);
FileUtils.writeByteArrayToFile(new File("/home/anindita/Desktop/BenchmarkGain1.png"), decodedBytes);
    }
    
}
