package com.linkedin.multipart;

import com.linkedin.r2.message.streaming.Writer;
import java.util.Map;


/**
 * Created by kvidhani on 7/6/15.
 */
//todo javadoc indicating to consumers that they should write a good deal of bytes...as many as possible really without taking a perf hit
  //otherwise the other side will just get a few bytes over and over
public interface MultiPartMIMEDataSource extends Writer {

  /**
   * Immediately return the headers need for this part. If no headers are provided, clients must return an empty list
   * @return
   */
  public Map<String, String> dataSourceHeaders();

}