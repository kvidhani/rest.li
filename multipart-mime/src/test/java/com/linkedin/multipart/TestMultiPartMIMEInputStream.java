package com.linkedin.multipart;

import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.streaming.WriteHandle;

import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.mail.internet.MimeBodyPart;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.linkedin.multipart.DataSources._bodyLessBody;
import static com.linkedin.multipart.DataSources._headerLessBody;
import static com.linkedin.multipart.DataSources._purelyEmptyBody;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.byteThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * Created by kvidhani on 7/17/15.
 */
public class TestMultiPartMIMEInputStream {

    private static ExecutorService executorService = Executors.newFixedThreadPool(5);

    @BeforeTest
    public void setup() {
        executorService.shutdownNow();
        executorService = Executors.newFixedThreadPool(5);
    }

    @AfterTest


    @DataProvider(name = "multipleInputStreams")
    public Object[][] multipleAbnormalBodies() throws Exception
    {

        //todo try with an input stream that throws if read is called and its finished()
        //apaprently byte array input stream doesn't mind providing 0 bytes when its finished
        final byte[] inputData = "some string".getBytes();
        // all the differnet n values
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(inputData);

        return new Object[][] {
                { inputData, new ByteArrayInputStream(outputStream.toByteArray()), 1, 1}
        };
    }

    //Test a successful consumption of an input stream to the write handle using the default
    //blocking timeouts and chunk sizes. Verify all of the logic in the InputStreamReader is excercised.
    @Test(dataProvider = "multipleInputStreams")
    public void testDefaultSuccessfulMultiPartMIMEInputStream(final byte[] inputData,
            final InputStream inputStream, final int onWritesNecessary,  int writeHandleCount) {

        //Setup:
        final WriteHandle writeHandle = Mockito.mock(WriteHandle.class);
        final MultiPartMIMEInputStream multiPartMIMEInputStream =
                new MultiPartMIMEInputStream.Builder(inputStream,  executorService, Collections.<String, String>emptyMap()).build();

        //We want to simulate a decreasing return from writeHandle.remaining().
        //I.e 3, 2, 1, 0....
        if (writeHandleCount > 1) {
            //This represents writeHandle.remaining() -> n, n -1, n - 2, .... 1, 0
            final Integer[] remainingWriteHandleCount = new Integer[writeHandleCount];
            int writeHandleCountTemp = writeHandleCount;
            for (int i = 0; i < writeHandleCount; i++) {
                remainingWriteHandleCount[i] = --writeHandleCountTemp;
            }
            when(writeHandle.remaining()).thenReturn(writeHandleCount, remainingWriteHandleCount);
        } else {
            //This represents writeHandle.remaining() -> 1, 0
            final Integer[] remainingWriteHandleCount = new Integer[] {0};
            when(writeHandle.remaining()).thenReturn(1, remainingWriteHandleCount);
        }

        //When data is written to the write handle, we append to the buffer
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final ByteString argument = (ByteString)invocation.getArguments()[0];
                appendByteStringToBuffer(byteArrayOutputStream, argument);
                return null;
            }
        }).when(writeHandle).write(any(ByteString.class));

        //Setup for done()
        final CountDownLatch doneLatch = new CountDownLatch(1);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                doneLatch.countDown();
                return null;
            }
        }).when(writeHandle).done();


        ///////////////////////////////////
        //Start things off

        //Init the data source
        multiPartMIMEInputStream.onInit(writeHandle);

        for (int i = 0; i<onWritesNecessary; i++) {
            multiPartMIMEInputStream.onWritePossible();
        }

        //Wait to finish
        try {
            boolean successful = doneLatch.await(10000, TimeUnit.MILLISECONDS);
            if (!successful) {
                Assert.fail("Timeout when waiting for input stream to completely transfer");
            }
        } catch (Exception exception) {
            Assert.fail("Unexpected exception when waiting for input stream to completely transfer");
        }

        ///////////////////////////////////
        //Assert
        Assert.assertEquals(byteArrayOutputStream.toByteArray(), inputData, "All data from the input stream should have successfully been transferred");



        //Mockito.verify(streamRequest, Mockito.times(1));
        //todo number of times? and assert it happened that many times?
        //verify that no more called

    }

    private void appendByteStringToBuffer(final ByteArrayOutputStream outputStream, final ByteString byteString)
    {
        try {
            outputStream.write(byteString.copyBytes());
        } catch (Exception e) {
            Assert.fail();
        }
    }
}
