package com.linkedin.multipart;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.message.rest.StreamResponse;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import test.r2.integ.AbstractStreamTest;

/**
 * Created by kvidhani on 7/11/15.
 */

//in rb mention:
//        1. using writer directly in your interface - i.e composite writer
//        2. the fact you are using the package private interface to hide public api details - could have also used abstract class….
//        3. the writer has three constructors (part, single and multi)
// 4. Can abort be called? MultiPartMIMEInputstream could be aborted in mid read...what to do? figure out what to do here
    //and come bacck to this problme. ask zhenak or ang



public abstract class AbstractMultiPartMIMEIntegrationStreamTest extends AbstractStreamTest {

    protected static Callback<StreamResponse> expectSuccessCallback(final CountDownLatch latch,
                                                                    final AtomicInteger status,
                                                                    final Map<String, String> headers)
    {
        return new Callback<StreamResponse>()
        {
            @Override
            public void onError(Throwable e)
            {
                latch.countDown();
            }

            @Override
            public void onSuccess(StreamResponse result)
            {
                status.set(result.getStatus());
                headers.putAll(result.getHeaders());
                latch.countDown();
            }
        };
    }
}
