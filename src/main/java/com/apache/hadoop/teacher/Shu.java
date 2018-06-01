package com.apache.hadoop.teacher;

import java.util.LinkedList;

public class Shu {
    public static void main(String[] args) {

        LinkedList<String> a=new LinkedList<>();
        for(int i=1;i<=10;i++){
            a.add("第"+i+"个数");
        }
        int count=0;
        while(true){
            if(a.size()==1){
                break;
            }
            count++;
            if(count%3==0){
                a.remove(a.getFirst());
            }else {
                //a.add();
            }

        }
        System.out.println(a.get(0));

    }
}
