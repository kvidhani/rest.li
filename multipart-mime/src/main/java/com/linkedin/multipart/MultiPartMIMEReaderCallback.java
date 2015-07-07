package com.linkedin.multipart;

/**
 * Created by kvidhani on 6/5/15.
 */
public interface MultiPartMIMEReaderCallback {
    //Upon construction of a MultiPartReader or upon a call to registerReaderCallback()
    //inside of the MultiPartMIMEReader this callback will be invoked
    //(if there are parts in this stream).
    //If a part had just finished, meaning onCurrentPartSuccessfullyFinished() was invoked
    //in the SinglePartMIMEReader callback, then this will be invoked if there are
    //future parts available.
    //Note that when this is called, it does not bind the callee to acquire and
    //commit to reading this part. Only upon registering with the SinglePartMIMEReader (init())
    //does the client then commit to reading that part.
    void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader);

    //When the entire stream is finished
    void onFinished();

    //When all parts are abandoned
    void onAbandoned();

    //When there is an error reading from the stream.
    //todo mention that this may be called when chaining a single part
    void onStreamError(Throwable e);
}
