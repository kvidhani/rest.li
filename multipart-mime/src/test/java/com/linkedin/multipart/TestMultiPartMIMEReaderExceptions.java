package com.linkedin.multipart;

import com.linkedin.r2.message.rest.StreamRequest;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;


/**
 * Created by kvidhani on 7/13/15.
 */
public class TestMultiPartMIMEReaderExceptions {


  @Test
  public void basicReadSandbox() {
    StreamRequest streamRequest = Mockito.mock(StreamRequest.class);
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn()
  }

  @Test
  public void with_arguments(){

    Comparable c= Mockito.mock(Comparable.class);
    when(c.compareTo("Test")).thenReturn(1);
    Assert.assertEquals(1, c.compareTo("Test"));
  }

}
