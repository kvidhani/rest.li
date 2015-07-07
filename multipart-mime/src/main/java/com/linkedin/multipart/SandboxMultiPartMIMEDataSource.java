package com.linkedin.multipart;

import com.linkedin.r2.message.streaming.Writer;
import java.util.Map;


/**
 * Created by kvidhani on 7/6/15.
 */
public interface SandboxMultiPartMIMEDataSource extends Writer {

  /**
   * Immediately return the headers need for this part. If no headers are provided, clients must return an empty list
   * @return
   */
  public Map<String, String> dataSourceHeaders();

}