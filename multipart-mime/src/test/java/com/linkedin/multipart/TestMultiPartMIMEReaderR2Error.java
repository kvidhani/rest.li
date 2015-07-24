package com.linkedin.multipart;

import com.linkedin.data.ByteString;
import com.linkedin.multipart.reader.exceptions.StreamFinishedException;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.ReadHandle;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.mail.internet.MimeMultipart;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.linkedin.multipart.DataSources._largeDataSource;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Created by kvidhani on 7/22/15.
 */
public class TestMultiPartMIMEReaderR2Error {

  private static ExecutorService threadPoolExecutor;

  MultiPartMIMEReader _reader;
  MultiPartMIMEAbandonReaderCallbackImpl _currentMultiPartMIMEReaderCallback;

  @BeforeTest
  public void setup() {
    threadPoolExecutor = Executors.newFixedThreadPool(5);
  }

  @AfterTest
  public void shutDown() {
    threadPoolExecutor.shutdownNow();
  }


  ///////////////////////////////////////////////////////////////////////////////////////


  //TOdo - change your other unit tests to not include that callback. you just need the latch really.


  //This test will verify that, in the middle of
  //Also when we are not in an erroneous state, i.e in the middle of normal processing
  @Test
  public void testMidProcessingR2Error() throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    multiPartMimeBody.addBodyPart(_largeDataSource);
    multiPartMimeBody.addBodyPart(_largeDataSource);
    multiPartMimeBody.addBodyPart(_largeDataSource);
    multiPartMimeBody.addBodyPart(_largeDataSource);
    multiPartMimeBody.addBodyPart(_largeDataSource);
    multiPartMimeBody.addBodyPart(_largeDataSource);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);
    final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());

    final String content = (String)_largeDataSource.getContent();
    //We want to read one byte, _readCount many times before stop. This way we ensure stop somewhere between a part.
    //This logic will have us stop somewhere in the middle of the 2nd part.
    SinglePartMIMEAbandonReaderCallbackImpl._readCount = (int)(Math.ceil(content.length() * 1.5));
    CountDownLatch countDownLatch =
        executeRequestPartialReadWithException(requestPayload, 1, multiPartMimeBody.getContentType());

    countDownLatch.await(60000, TimeUnit.MILLISECONDS);

    //When this returns, its partially complete
    //In this point in time let us simulate an R2 error
    _reader.getR2MultiPartMIMEReader().onError(new NullPointerException());

    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._streamError instanceof NullPointerException);
    try {
      _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.get(0)._singlePartMIMEReader.requestPartData();
      Assert.fail();
    } catch (StreamFinishedException streamFinishedException) {
      //pass
    }

    Assert.assertEquals(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(), 2);
    Assert.assertNull(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.get(0)._streamError);
    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks
        .get(1)._streamError instanceof NullPointerException);

    try {
      _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.get(1)._singlePartMIMEReader.requestPartData();
      Assert.fail();
    } catch (StreamFinishedException streamFinishedException) {
      //pass
    }
  }




  ///////////////////////////////////////////////////////////////////////////////////////

  private CountDownLatch executeRequestPartialReadWithException(final ByteString requestPayload, final int chunkSize,
      final String contentTypeHeader) throws Exception {


    final EntityStream entityStream = mock(EntityStream.class);
    final ReadHandle readHandle = mock(ReadHandle.class);

    //We have to use the AtomicReference holder technique to modify the current remaining buffer since the inner class
    //in doAnswer() can only access final variables.
    final AtomicReference<MultiPartMIMEReader.R2MultiPartMIMEReader> r2Reader =
        new AtomicReference<MultiPartMIMEReader.R2MultiPartMIMEReader>();

    //This takes the place of VariableByteStringWriter if we were to use R2 directly.
    final VariableByteStringViewer variableByteStringViewer = new VariableByteStringViewer(requestPayload, chunkSize);

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        final MultiPartMIMEReader.R2MultiPartMIMEReader reader = r2Reader.get();
        Object[] args = invocation.getArguments();

        //will always be 1 since MultiPartMIMEReader only does _rh.request(1)
        final int chunksRequested = (Integer)args[0];

        for (int i = 0;i<chunksRequested; i++) {

          //Our tests will run into a stack overflow unless we use a thread pool here to fire off the callbacks.
          //Especially in cases where the chunk size is 1. When the chunk size is one, the MultiPartMIMEReader
          //ends up doing many _rh.request(1) since each write is only 1 byte.
          //R2 uses a different technique to avoid stack overflows here which is unnecessary to emulate.
          threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {

              ByteString clientData = variableByteStringViewer.onWritePossible();
              if (clientData.equals(ByteString.empty())) {
                reader.onDone();
              }
              else {
                reader.onDataAvailable(clientData);
              }

            }
          });

        }

        return null;
      }
    }).when(readHandle).request(isA(Integer.class));

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        final MultiPartMIMEReader.R2MultiPartMIMEReader reader = (MultiPartMIMEReader.R2MultiPartMIMEReader) args[0];
        r2Reader.set(reader);
        //R2 calls init immediately upon setting the reader
        reader.onInit(readHandle);
        return null;
      }
    }).when(entityStream).setReader(isA(MultiPartMIMEReader.R2MultiPartMIMEReader.class));

    final StreamRequest streamRequest = mock(StreamRequest.class);
    when(streamRequest.getEntityStream()).thenReturn(entityStream);

    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);

    final AtomicReference<Throwable> throwable = new AtomicReference<Throwable>();
    final CountDownLatch latch = new CountDownLatch(1);


    _reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);
    _currentMultiPartMIMEReaderCallback=
        new MultiPartMIMEAbandonReaderCallbackImpl(latch);
    _reader.registerReaderCallback(_currentMultiPartMIMEReaderCallback);

  return latch;

  }





  private static class SinglePartMIMEAbandonReaderCallbackImpl implements SinglePartMIMEReaderCallback {

    final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    Throwable _streamError = null;
    final CountDownLatch _countDownLatch;

    //We want to read one byte, _readCount many times before stop. This way we ensure stop somewhere between a part.
    static int _readCount;

    SinglePartMIMEAbandonReaderCallbackImpl(final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader, final CountDownLatch countDownLatch) {
      _singlePartMIMEReader = singlePartMIMEReader;
      _countDownLatch = countDownLatch;


    }

    @Override
    public void onPartDataAvailable(ByteString b) {
      if(_readCount-- > 0) {
        _singlePartMIMEReader.requestPartData();
      } else {
        _countDownLatch.countDown();
      }
    }

    @Override
    public void onFinished() {
    }

    //Delegate to the top level for now for these two
    @Override
    public void onAbandoned() {
      Assert.fail();
    }

    @Override
    public void onStreamError(Throwable e) {
      _streamError = e;
    }
  }

  private static class MultiPartMIMEAbandonReaderCallbackImpl implements MultiPartMIMEReaderCallback {

    final List<SinglePartMIMEAbandonReaderCallbackImpl> _singlePartMIMEReaderCallbacks =
        new ArrayList<SinglePartMIMEAbandonReaderCallbackImpl>();
    Throwable _streamError = null;
    final CountDownLatch _latch;

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {

      SinglePartMIMEAbandonReaderCallbackImpl singlePartMIMEReaderCallback =
          new SinglePartMIMEAbandonReaderCallbackImpl(singlePartMIMEReader, _latch);
      singlePartMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
      _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);

      singlePartMIMEReader.requestPartData();
    }

    @Override
    public void onFinished() {
      Assert.fail();
    }

    @Override
    public void onAbandoned() {
      Assert.fail();
    }

    @Override
    public void onStreamError(Throwable e) {
      _streamError = e;
    }

    MultiPartMIMEAbandonReaderCallbackImpl(final CountDownLatch latch) {
      _latch = latch;
    }
  }





}
