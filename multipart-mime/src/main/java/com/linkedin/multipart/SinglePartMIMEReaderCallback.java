package com.linkedin.multipart;

import com.linkedin.data.ByteString;

/**
 * Created by kvidhani on 6/5/15.
 */
public interface SinglePartMIMEReaderCallback {
    //When data is available to be read on the current part
    void onPartDataAvailable(ByteString b);

    //When the current part is finished being read
    void onFinished();

    //When the current part is finished being abandoned.
    void onAbandoned();

    //When there is an error reading from the stream.
    void onStreamError(Throwable e);
}
