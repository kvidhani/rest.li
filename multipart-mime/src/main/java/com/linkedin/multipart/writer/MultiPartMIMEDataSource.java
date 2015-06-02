package com.linkedin.multipart.writer;

import java.util.Map;


/**
 * Created by kvidhani on 5/18/15.
 */
//todo redo javadocs

public interface MultiPartMIMEDataSource {

  /**
   * Init with the DataSourceHandle to invoke once bytes are ready to provide.
   */
  void onInit(DataSourceHandle dataSourceHandle);

  void onWritePossible();

  void onAbort(Throwable e);

  /**
   * Immediately return the headers need for this part.
   * @return
   */
  Map<String, String> dataSourceHeaders();

}
