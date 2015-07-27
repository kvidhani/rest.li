package com.linkedin.multipart;

import com.linkedin.r2.message.streaming.Writer;
import java.util.Map;


/**
 * @author Karim Vidhani
 *
 * Interface that is required to be implemented by custom data sources for the
 * {@link com.linkedin.multipart.MultiPartMIMEWriter}.
 *
 */
public interface MultiPartMIMEDataSource extends Writer {

  /**
   * Immediately return the headers need for this part. If no headers are provided, clients must
   * return an empty map.
   *
   * @return a Map representing the headers for this data source.
   */
  public Map<String, String> dataSourceHeaders();
}