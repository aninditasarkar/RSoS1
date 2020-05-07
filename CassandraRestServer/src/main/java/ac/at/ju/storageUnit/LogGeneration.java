/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.storageUnit;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
/**
 *
 * @author dsg
 */
public class LogGeneration {
    
 public static FileHandler fh = null;
 
 public static void init(){
 try {
 fh=new FileHandler("ServerStatus.log", true);
 } catch (Exception e) {
 e.printStackTrace();
 }
 Logger l = Logger.getLogger("");
 fh.setFormatter(new SimpleFormatter());
 l.addHandler(fh);
 l.setLevel(Level.CONFIG);
 fh.close();
 }
 
 
 /*public static void main(String[] args) {
 LogGeneration.init();
 
 logger.log(Level.INFO, "message 1");
 logger.log(Level.SEVERE, "message 2");
 logger.log(Level.FINE, "message 3");
 }*/
}
    

