/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.test;

/**
 *
 * @author anindita
 */


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Base64;

public class FileWrite {
    public String imageTStringConversion()throws Exception
    {
        InputStream inputStream = null;
        inputStream = new FileInputStream("/home/anindita/Desktop/BenchmarkGain.png");
                //inputStream =new ByteArrayInputStream("hello".getBytes());
                
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
         String output=Base64.getEncoder().encodeToString(sb.toString().getBytes());
         return output;
    }
    public static void main(String[] args) throws Exception {

	InputStream inputStream = null;
	OutputStream outputStream = null;

	try {
		// read this file into InputStream
		
                inputStream =new ByteArrayInputStream(new FileWrite().imageTStringConversion().getBytes());
                

		// write the inputStream to a FileOutputStream
		outputStream = 
                    new FileOutputStream(new File("/home/anindita/Desktop/test.png"));

		int read1 = 0;
		byte[] bytes = new byte[1024];

		while ((read1 = inputStream.read(bytes)) != -1) {
			outputStream.write(bytes, 0, read1);
		}

		System.out.println("Done!");

	} catch (IOException e) {
		e.printStackTrace();
	} finally {
		if (inputStream != null) {
			try {
				inputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (outputStream != null) {
			try {
				// outputStream.flush();
				outputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}
    }
}
