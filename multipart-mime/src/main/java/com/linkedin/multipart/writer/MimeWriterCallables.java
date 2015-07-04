package com.linkedin.multipart.writer;

import java.util.concurrent.Callable;


/**
 * Created by kvidhani on 7/2/15.
 */
//Function pointer to use in our iterative callback invocation scheme in MultiPartMIMEWriter
//to avoid stack overflows.
  //todo note that these won't return to the same bit of code when called!!
  //remember the bug you had
public class MimeWriterCallables {

  //MultiPartMIMEDataSource callable wrappers:
  static class onWritePossibleCallable implements Callable<Void> {

    final MultiPartMIMEDataSource _mimeDataSource;
    @Override
    public Void call() throws Exception {
      _mimeDataSource.onWritePossible();
      return null; //This is ignored.
    }

    onWritePossibleCallable(final MultiPartMIMEDataSource mimeDataSource) {
      _mimeDataSource = mimeDataSource;
    }
  }
}

