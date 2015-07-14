package com.linkedin.multipart;

import org.testng.annotations.Test;


/**
 * Created by kvidhani on 7/13/15.
 */
public class TestMultiPartMIMEReaderExceptions {

  @Test
  public void with_arguments(){

    Comparable c= mock(Comparable.class);
    when(c.compareTo("Test")).thenReturn(1);
    assertEquals(1,c.compareTo("Test"));
  }

}
