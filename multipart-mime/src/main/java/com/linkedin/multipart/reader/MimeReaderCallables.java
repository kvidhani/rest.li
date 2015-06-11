package com.linkedin.multipart.reader;

import com.linkedin.data.ByteString;
import java.util.concurrent.Callable;


/**
 * Created by kvidhani on 6/10/15.
 */
//These are all essentially function pointers to use in our iterative callback invocation scheme in MultiPartMIMEReader
//to avoid stack overflows.
public class MimeReaderCallables {

  //SinglePartMIMEReaderCallback callable wrappers:
  static class onPartDataCallable implements Callable<Void> {

    final SinglePartMIMEReaderCallback _singlePartMIMEReaderCallback;
    final ByteString _data;
    @Override
    public Void call() throws Exception {
      _singlePartMIMEReaderCallback.onPartDataAvailable(_data);
      return null; //This is ignored.
    }

    onPartDataCallable(final SinglePartMIMEReaderCallback singlePartMIMEReaderCallback, final ByteString data) {
      _singlePartMIMEReaderCallback = singlePartMIMEReaderCallback;
      _data = data;
    }
  }

  static class onFinishedCallable implements Callable<Void> {

    final SinglePartMIMEReaderCallback _singlePartMIMEReaderCallback;
    @Override
    public Void call() throws Exception {
      _singlePartMIMEReaderCallback.onFinished();
      return null; //This is ignored.
    }

    onFinishedCallable(final SinglePartMIMEReaderCallback singlePartMIMEReaderCallback) {
      _singlePartMIMEReaderCallback = singlePartMIMEReaderCallback;
    }
  }

  static class onAbandonedCallable implements Callable<Void> {

    final SinglePartMIMEReaderCallback _singlePartMIMEReaderCallback;
    @Override
    public Void call() throws Exception {
      _singlePartMIMEReaderCallback.onAbandoned();
      return null; //This is ignored.
    }

    onAbandonedCallable(final SinglePartMIMEReaderCallback singlePartMIMEReaderCallback) {
      _singlePartMIMEReaderCallback = singlePartMIMEReaderCallback;
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////
  //MultiPartMIMEReader callable wrappers:

  static class onNewPartCallable implements Callable<Void> {

    final MultiPartMIMEReaderCallback _multiPartMIMEReaderCallback;
    final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    @Override
    public Void call() throws Exception {
      _multiPartMIMEReaderCallback.onNewPart(_singlePartMIMEReader);
      return null; //This is ignored
    }

    onNewPartCallable(final MultiPartMIMEReaderCallback multiPartMIMEReaderCallback,
                      final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {
      _multiPartMIMEReaderCallback = multiPartMIMEReaderCallback;
      _singlePartMIMEReader = singlePartMIMEReader;
    }

  }
}
