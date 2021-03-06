/*
   Copyright (c) 2015 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.linkedin.multipart;


import com.linkedin.data.ByteString;
import com.linkedin.multipart.utils.VariableByteStringViewer;
import com.linkedin.r2.filter.R2Constants;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.ReadHandle;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Provides the setup and teardown of a {@link java.util.concurrent.ExecutorService} that is necessary
 * for most tests.
 *
 * @author Karim Vidhani
 */
public abstract class AbstractMIMEUnitTest
{
  protected static ScheduledExecutorService _scheduledExecutorService;
  protected static int TEST_TIMEOUT = 30000;
  //The following are mock objects used when we test the reader using a mocked version of R2.
  protected EntityStream entityStream;
  protected ReadHandle readHandle;
  protected StreamRequest streamRequest;

  @BeforeClass
  public void threadPoolSetup()
  {
    //Gradle could run multiple test suites concurrently so we need plenty of threads.
    _scheduledExecutorService = Executors.newScheduledThreadPool(30);
  }

  @AfterClass
  public void threadPoolTearDown()
  {
    _scheduledExecutorService.shutdownNow();
    TEST_TIMEOUT = 30000; //In case a subclass changed this
  }

  //This is used by a lot of tests so we place it here.
  @DataProvider(name = "chunkSizes")
  public Object[][] chunkSizes() throws Exception
  {
    return new Object[][]{{1}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE}};
  }

  //This is used when we need to mock out R2 and write a payload for our reader to read
  protected void mockR2AndWrite(final ByteString payload, final int chunkSize, final String contentType)
  {
    entityStream = mock(EntityStream.class);
    readHandle = mock(ReadHandle.class);
    streamRequest = mock(StreamRequest.class);

    //We have to use the AtomicReference holder technique to modify the current remaining buffer since the inner class
    //in doAnswer() can only access final variables.
    final AtomicReference<MultiPartMIMEReader.R2MultiPartMIMEReader> r2Reader =
        new AtomicReference<MultiPartMIMEReader.R2MultiPartMIMEReader>();

    //This takes the place of VariableByteStringWriter if we were to use R2 directly.
    final VariableByteStringViewer variableByteStringViewer = new VariableByteStringViewer(payload, chunkSize);

    doAnswer(new Answer()
    {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable
      {
        final MultiPartMIMEReader.R2MultiPartMIMEReader reader = r2Reader.get();
        Object[] args = invocation.getArguments();

        //will always be 1 since MultiPartMIMEReader only does _rh.request(1)
        final int chunksRequested = (Integer) args[0];

        for (int i = 0; i < chunksRequested; i++)
        {
          //Our tests will run into a stack overflow unless we use a thread pool here to fire off the callbacks.
          //Especially in cases where the chunk size is 1. When the chunk size is one, the MultiPartMIMEReader
          //ends up doing many _rh.request(1) since each write is only 1 byte.
          //R2 uses a different technique to avoid stack overflows here which is unnecessary to emulate.
          _scheduledExecutorService.submit(new Runnable()
          {
            @Override
            public void run()
            {
              ByteString clientData = variableByteStringViewer.onWritePossible();
              if (clientData.equals(ByteString.empty()))
              {
                reader.onDone();
              }
              else
              {
                reader.onDataAvailable(clientData);
              }
            }
          });
        }

        return null;
      }
    }).when(readHandle).request(isA(Integer.class));

    //We need a final version of the read handle since its passed to an inner class below.
    final ReadHandle readHandleRef = readHandle;
    doAnswer(new Answer()
    {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable
      {
        Object[] args = invocation.getArguments();
        final MultiPartMIMEReader.R2MultiPartMIMEReader reader = (MultiPartMIMEReader.R2MultiPartMIMEReader) args[0];
        r2Reader.set(reader);
        //R2 calls init immediately upon setting the reader
        reader.onInit(readHandleRef);
        return null;
      }
    }).when(entityStream).setReader(isA(MultiPartMIMEReader.R2MultiPartMIMEReader.class));

    when(streamRequest.getEntityStream()).thenReturn(entityStream);
    final String contentTypeHeader =
        contentType + ";somecustomparameter=somecustomvalue" + ";anothercustomparameter=anothercustomvalue";
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);
  }
}