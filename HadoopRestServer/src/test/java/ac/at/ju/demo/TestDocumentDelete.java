/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.at.ju.demo;

import ac.at.ju.databaseDesign.HadoopJSONServer;



/**
 *
 * @author dsg
 */
public class TestDocumentDelete {
    public static void main(String []p)
    {
        String json="{\n" +
"   \"account\":\"i198\",\n" +
"   \"container\":\"ecg\",\n" +
"   \"eliminate\":{\n" +
"      \"id\":\"BenchmarkGain1\",\n" +
"      \"fileextension\":\"png\",\n"+

"   }\n" +
"}";
       HadoopJSONServer obj=new HadoopJSONServer();
       System.out.println(obj.hadoopDelete(json));
    }
}
