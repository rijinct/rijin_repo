package com.nokia.web.controller;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class Test {

	public enum Season { Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec }  
	public static void main(String[] args) {
		Calendar c = Calendar.getInstance();
		
				c.set(2016, 0, 28);
		while (c.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY) {
			c.add(Calendar.DATE, -1);
		}
		
		test();
	}

	public static void test(){
		String a = "A";
		
		Map<String, String> map1 = new HashMap<String, String>();
		map1.put(a, a);
		map1.put("B", "B");
		
		Map<String, String> map2 = new HashMap<String, String>();
		map2.put("A", "A");
		map2.put("B", "B");
		
		Map<String, String> map3 = new HashMap<String, String>();
		map3.put("B", "B");
		map3.put("A", "A");
		
		Map<Object, Object> map4 = new HashMap<Object, Object>();
		map4.put("A", "A");
		map4.put("B", "B");
		
		System.out.println(map1.equals(map2));
		System.out.println(map2.equals(map3));
		System.out.println(map3.equals(map4));
		System.out.println(map1.equals(map4));
		
		
		System.out.println(map3);
		
		
		
		 System.out.println("main has started.");
         
         MyThread thread1=new MyThread();
         thread1.run();;
         
         System.out.println("main has ended.");
 	}
}

class MyThread extends Thread {
	 
  
    
}


