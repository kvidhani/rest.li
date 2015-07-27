package com.linkedin.multipart;

import com.linkedin.data.ByteString;

/**
 * Created by kvidhani on 6/5/15.
 */
public interface SinglePartMIMEReaderCallback {
    //When data is available to be read on the current part
    public void onPartDataAvailable(ByteString b);

    //When the current part is finished being read
    public void onFinished();

    //When the current part is finished being abandoned.
    public void onAbandoned();

    //When there is an error reading from the stream.
    //Mention this can be called AT ANYTIME...because R2 may call this!
    public void onStreamError(Throwable e);
}
