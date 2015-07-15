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
