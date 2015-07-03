package com.linkedin.multipart.writer;

import java.util.Map;


/**
 * Created by kvidhani on 5/18/15.
 */
//todo redo javadocs
public interface MultiPartMIMEDataSource {

  /**
   * Init with the DataSourceHandle to invoke once bytes are ready to provide. This will only be called
   * when its this data source's turn.
   *
   * Note that implementations should do all of their setup here or in the constructor. Its up to them.
   * Note that onWritePossible() can be called immediately after onInit and if some database connection took a while to
   * setup in onInit then there may be a race condition with onWritePossible.
   */
  void onInit(MultiPartMIMEWriter.DataSourceHandleImpl dataSourceHandle);

  void onWritePossible();

  //This signifies to the data source that the stream has been aborted. This will be called to all data sources,
  //regardless of if onInit() was called earlier with the exception of data sources which have already finished (meaning
  //they called onDone() on the DataSourceHandle already)
  void onAbort(Throwable e);

  /**
   * Immediately return the headers need for this part. If no headers are provided, clients must return an empty list
   * @return
   */
  Map<String, String> dataSourceHeaders();

}
