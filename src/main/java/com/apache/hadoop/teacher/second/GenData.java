package com.apache.hadoop.teacher.second;

import java.io.FileOutputStream;
import java.io.PrintWriter;

public class GenData {
    public static void main(String[] args) throws  Exception{
        String[] severs={"a1","b","c","d"};
        String[] roles = {"role1","role2","role3","role4","role5","role6","role7","role8","role9","role10"};
        PrintWriter out = new PrintWriter(new FileOutputStream("E:\\eg\\datas.txt"));
        String server="";
        String role="";
        for (int i=0;i<5000;i++){
            server=severs[(int)(Math.random()*severs.length)];
            role = roles[(int)(Math.random()*roles.length)];
            int m = (int)(Math.random()*200+1);
            out.println(server+","+role+","+m);
            out.flush();
        }
        out.close();
        System.out.println("0k.....");
    }
}
