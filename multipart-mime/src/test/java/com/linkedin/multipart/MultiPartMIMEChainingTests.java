package com.linkedin.multipart;

/**
 * Created by kvidhani on 7/22/15.
 */
public class MultiPartMIMEChainingTests {



    private static void recurse(int i, String s) {
      try {
        recurse(i+1, "foo");
      } catch (StackOverflowError e) {
        System.out.print("Recursion depth on this system is " + i + ".");
      }
    }

    public static void main(String[] args) {
      recurse(0, "foo");
    }



}
