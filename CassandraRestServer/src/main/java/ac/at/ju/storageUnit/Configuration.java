/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.storageUnit;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Properties;
import java.util.logging.FileHandler;
//import java.util.logging.Level;
//import java.util.logging.Logger;



public class Configuration {
    
    
    
    
    public Configuration() {
    }

    public static String getConfig(String configureName) {

       
      
        String path = Configuration.class.getProtectionDomain().getCodeSource().getLocation().getPath();
       // LogGeneration.init();
        //Logger logger = Logger.getLogger(LogGeneration.class.getName());
       
         //System.out.println("path: " + path);
        int index = path.indexOf("/classes");  //from main() method and ubuntu browser
        //int index = path.indexOf("/classes/ac/at");  //from browser
        
        path = path.substring(0, index);
        
        try {
            path = URLDecoder.decode(path, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            //System.out.println("Exception occured  :"+ex);
           // logger.log(Level.SEVERE, null, ex);
        }
      
        
        //logger.log(Level.INFO, path);
        
        Properties prop = new Properties();
        String configString = "";
        InputStream input = null;

        try {

            //input = new FileInputStream(path + "/CassandraRestServer/WEB-INF/Configuration.properties"); //for calling the main() 
            input = new FileInputStream(path + "/Configuration.properties"); //from rest api
            // load a properties file
            prop.load(input);
           configString=prop.getProperty(configureName);
            

        } catch (IOException ex) {
             //logger.log(Level.SEVERE, null, ex);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
               //      logger.log(Level.SEVERE, null, e);
                }
            }
        }

        return configString;
    }
    
   
}

