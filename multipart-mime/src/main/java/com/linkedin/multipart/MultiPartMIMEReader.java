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
import com.linkedin.multipart.exceptions.IllegalMultiPartMIMEFormatException;
import com.linkedin.multipart.exceptions.PartBindException;
import com.linkedin.multipart.exceptions.PartFinishedException;
import com.linkedin.multipart.exceptions.PartNotInitializedException;
import com.linkedin.multipart.exceptions.ReaderNotInitializedException;
import com.linkedin.multipart.exceptions.StreamBusyException;
import com.linkedin.multipart.exceptions.ReaderFinishedException;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.util.LinkedDeque;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;

import org.apache.commons.lang.ArrayUtils;


/**
 * Async streaming multipart mime reader based on the official RFC.
 *
 * This class uses R2 streaming and a look-ahead buffer to allow clients to walk through all the part data using an async,
 * callback based approach.
 *
 * Clients must first create a callback of type {@link com.linkedin.multipart.MultiPartMIMEReaderCallback} to pass to
 * {@link MultiPartMIMEReader#registerReaderCallback(com.linkedin.multipart.MultiPartMIMEReaderCallback)}. This is the first
 * step to using the MultiPartMIMEReader.
 *
 * Upon registration, at some time in the future, MultiPartMIMEReader will create {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader}s
 * which will be passed to {@link MultiPartMIMEReaderCallback#onNewPart(com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader)}.
 *
 * Clients will then have to create an instance of {@link com.linkedin.multipart.SinglePartMIMEReaderCallback} to bind
 * and commit to reading these parts.
 *
 * Note that NONE of the APIs in this class are thread safe. Furthermore it is to be noted that API calls must be event driven.
 * For example, asking for more part data using {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader#requestPartData()}
 * can only be done either upon binding to the {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader} or
 * after receiving data on {@link SinglePartMIMEReaderCallback#onPartDataAvailable(com.linkedin.data.ByteString)}. Therefore
 * attempting to queue multiple reads, instead of waiting for callback invocations to drive forward, will result in runtime exceptions.
 *
 * @author Karim Vidhani
 */
public final class MultiPartMIMEReader
{
  private final R2MultiPartMIMEReader _reader;
  private final EntityStream _entityStream;
  private volatile MultiPartMIMEReaderCallback _clientCallback;
  private volatile String _preamble;
  private volatile MultiPartReaderState _multiPartReaderState;
  private volatile SinglePartMIMEReader _currentSinglePartMIMEReader;

  class R2MultiPartMIMEReader implements Reader
  {
    private volatile ReadHandle _rh;
    private volatile List<Byte> _byteBuffer = new ArrayList<Byte>();
    //The reason for the first boundary vs normal boundary difference is because the first boundary MAY be missing the
    //leading CRLF.
    //No even though it is incorrect for a client to send a multipart/mime in this manner,
    //the RFC states that readers should be tolerant and be able to handle such cases.
    private final String _firstBoundary;
    private final String _normalBoundary;
    private final String _finishingBoundary;
    private final List<Byte> _firstBoundaryBytes = new ArrayList<Byte>();
    private final List<Byte> _normalBoundaryBytes = new ArrayList<Byte>();
    private final List<Byte> _finishingBoundaryBytes = new ArrayList<Byte>();
    private volatile boolean _firstBoundaryEvaluated = false;
    //A signal from the R2 reader has been notified that all data is done being sent over. This does NOT mean that our
    //top level reader can be notified that they are done since data could still be in the buffer.
    private volatile boolean _r2Done = false;

    //These two fields are needed to support our iterative invocation of callbacks so that we don't end up with a recursive loop
    //which would lead to a stack overflow.
    private final Queue<Callable> _callbackQueue = new LinkedDeque<Callable>();
    private volatile boolean _callbackInProgress = false;

    ///////////////////////////////////////////////////////////////////////////////////////////////////
    //Reader interface implementation

    @Override
    public void onInit(ReadHandle rh)
    {
      _rh = rh;
      //Start the reading process since the top level callback has been bound.
      //Note that we read ahead here and we read only what we need. Our policy is always to read only 1 chunk at a time.
      _rh.request(1);
    }

    @Override
    public void onDataAvailable(ByteString data)
    {
      //A response for our _rh.request(1) has come
      processEventAndInvokeClient(data);
    }

    @Override
    public void onDone()
    {
      //Be careful, we still could have space left in our buffer.
      _r2Done = true;
      //We need to trigger onDataAvailable() again with empty data because there is an outstanding request
      //to _rh.request(1).
      onDataAvailable(ByteString.empty());
    }

    @Override
    public void onError(Throwable e)
    {
      //R2 has informed us of an error. So we notify our readers and shut things down.

      //It is important to note that R2 will only call onError only once we exit onDataAvailable().
      //Therefore there are no concurrency issues to be concerned with. If we are going back and forth and honoring
      //client requests using data from memory we will eventually need to ask R2 for more data. At this point,
      //onError() will be called by R2 and we can clean up state and notify our clients on their callbacks.

      //It could be the case that we already finished, or reach an erroneous state earlier on our own
      //(i.e malformed multipart mime body or a client threw an exception when we invoked their callback).
      //In such a case, just return.
      if (_multiPartReaderState == MultiPartReaderState.FINISHED)
      {
        return;
      }

      handleExceptions(e);
    }

    ///////////////////////////////////////////////////////////////////////////////////////
    //Core look-ahead buffering and callback logic begins here:

    //Client APIs invoke this method
    private void processEventAndInvokeClient()
    {
      processEventAndInvokeClient(ByteString.empty());
    }

    //OnDataAvailable() will invoke this method
    private void processEventAndInvokeClient(final ByteString data)
    {
      //Note that only one thread should ever be calling processEventAndInvokeClient() at any time.
      //We control invocation of this this method using the various states.
      //Client API calls (from the readers) will call processEventAndInvokeClient() to refresh the logic and drive forward
      //in a forcefully sequential manner.
      //When more data is needed to fulfill a client's request, we will ask R2 and R2 will call us sequentially as well.
      //During the time we are waiting for R2 to fulfill our request, client API calls cannot do anything further.
      //It is for this reason we don't have to synchronize any data used within this method. This is a big
      //performance win.
      //
      //Anything thrown by us will can it to R2 or to our clients. This should ideally never ever happen and if it
      //happens its a bug.
      //If R2 does receive a RuntimeException from us, Regardless, if this was the case and we throw to R2 then:
      //A. In case of the server, R2 send back a 500 internal server error.
      //B. In case of the client, R2 will close the connection.
      //
      //Also note that look ahead buffering uses an ArrayList<Byte>. When we need to consume data from the
      //_byteBuffer, we create a sublist() representing the data to be consumed. Once the data is consumed we
      //must update the _byteBuffer to forget about these bytes. We can't simply update _byteBuffer to point
      //to a new sublist() that is offset from the original because sublist() maintains a reference to the parent
      //list thereby preventing GC.
      //Hence we have two options on updating the buffer to remove this data:
      //1. Calling clear() on the sublist which we just consumed. This will clear the bytes in the parent list.
      //However it seems that most sublists created from parent lists do not allow mutations and therefore will throw
      //ConcurrentModificationException. Hence we cannot use this option.
      //2. Copying the data from the offset we need going forward into a new ArrayList<Byte> and setting that to
      //_byteBuffer. This leaves the original list to be GC'd.
      //
      //For the current implementation we have no choice but to use option 2. It may be a pragmatic exercise in the
      //future to write a custom mutable byte buffer or to research more off the shelf options.

      if (checkAndProcessEpilogue())
      {
        return;
      }

      if (checkAndProcessAbandonment())
      {
        return;
      }

      //Read data into our local buffer for further processing. All subsequent operations require this.
      appendByteStringToBuffer(data);

      if (checkAndProcessPreamble())
      {
        return;
      }

      if (checkForSufficientBufferSize())
      {
        return;
      }

      //Since this is the last step we end up returning anyway.
      performPartReading();
    }

    private void appendByteStringToBuffer(final ByteString byteString)
    {
      final byte[] byteStringArray = byteString.copyBytes();
      for (final byte b : byteStringArray)
      {
        _byteBuffer.add(b);
      }
    }

    //This method is use to iteratively invoke our callbacks to prevent a stack overflow.
    private void processAndInvokeCallableQueue()
    {
      //This variable indicates that there is no current iterative invocation taking place. We can start one here.
      _callbackInProgress = true;

      while (!_callbackQueue.isEmpty())
      {
        final Callable<Void> callable = _callbackQueue.poll();
        try
        {
          callable.call();
        }
        catch (Throwable clientCallbackException)
        {
          handleExceptions(clientCallbackException);
        }
      }
      _callbackInProgress = false;
    }

    private boolean checkAndProcessEpilogue()
    {
      //Drop the epilogue on the ground. No need to read into our buffer.
      if (_multiPartReaderState == MultiPartReaderState.READING_EPILOGUE)
      {
        if (_r2Done)
        {
          //If r2 has already notified we are done, we can wrap up. There is no need to use our
          //iterative technique to call this callback because a client cannot possibly invoke us again.
          _multiPartReaderState = MultiPartReaderState.FINISHED;
          try
          {
            //This can throw so we need to notify the client that their APIs threw an exception when we invoked them.
            MultiPartMIMEReader.this._clientCallback.onFinished();
          }
          catch (RuntimeException clientCallbackException)
          {
            handleExceptions(clientCallbackException);
          }
          finally
          {
            return true; //Regardless of whether the invocation to onFinished() threw or not we need to return here
          }
        }
        //Otherwise r2 has not notified us that we are done. So we keep getting more bytes and dropping them.
        _rh.request(1);
        return true;
      }

      return false;
    }

    private boolean checkAndProcessAbandonment()
    {
      //Drop bytes for a top level abort.
      if (_multiPartReaderState == MultiPartReaderState.ABANDONING)
      {
        if (_r2Done)
        {
          //If r2 has already notified we are done, we can wrap up. No need to look at remaining bytes in buffer.
          //Also there is no need to use our iterative technique to call this callback because a client cannot
          //possibly invoke us again.
          _multiPartReaderState = MultiPartReaderState.FINISHED;
          try
          {
            //This can throw so we need to notify the client that their APIs threw an exception when we invoked them.
            MultiPartMIMEReader.this._clientCallback.onAbandoned();
          }
          catch (RuntimeException clientCallbackException)
          {
            handleExceptions(clientCallbackException);
          }
          finally
          {
            return true; //Regardless of whether the invocation to onFinished() threw or not we need to return here
          }
        }
        //Otherwise we keep on chugging forward and dropping bytes.
        _rh.request(1);
        return true;
      }

      return false;
    }

    private boolean checkAndProcessPreamble()
    {
      //Read the preamble in.
      if (_multiPartReaderState == MultiPartReaderState.CALLBACK_BOUND_AND_READING_PREAMBLE)
      {
        final int firstBoundaryLookup = Collections.indexOfSubList(_byteBuffer, _firstBoundaryBytes);
        final int lastBoundaryLookup = Collections.indexOfSubList(_byteBuffer, _finishingBoundaryBytes);
        //Before reading the preamble, check to see if this is an empty multipart mime envelope. This can be checked by
        //examining if the location of the first boundary matches the location of the finishing boundary.
        //We also have to take into consideration that the first boundary doesn't including the leading CRLF so we subtract
        //the length of the CRLF when doing the comparison.
        //This means that the envelope looked like:
        //Content-Type: multipart/<subType>; boundary="someBoundary"
        //
        //--someBoundary--
        if (firstBoundaryLookup - MultiPartMIMEUtils.CRLF_BYTES.length == lastBoundaryLookup)
        {
          //In such a case we need to let the client know that reading is complete since there were no parts.
          //There is no need to use our iterative technique to call this callback because a
          //client cannot possibly invoke us again.
          _multiPartReaderState = MultiPartReaderState.FINISHED;
          try
          {
            //This can throw so we need to notify the client that their APIs threw an exception when we invoked them.
            MultiPartMIMEReader.this._clientCallback.onFinished();
          }
          catch (RuntimeException clientCallbackException)
          {
            handleExceptions(clientCallbackException);
          }
          finally
          {
            return true; //Regardless of whether the invocation to onFinished() threw or not we need to return here
          }
        }

        //Otherwise proceed
        if (firstBoundaryLookup > -1)
        {
          //The boundary has been found. Everything up until this point is the preamble.
          final List<Byte> preambleBytes = _byteBuffer.subList(0, firstBoundaryLookup);
          _preamble = new String(ArrayUtils.toPrimitive(preambleBytes.toArray(new Byte[0])));

          //Make a new copy with the bytes we need leaving the old list to be GC'd
          _byteBuffer = new ArrayList<Byte>(_byteBuffer.subList(firstBoundaryLookup, _byteBuffer.size()));

          //We can now transition to normal reading.
          _multiPartReaderState = MultiPartReaderState.READING_PARTS;
        }
        else
        {
          //The boundary has not been found in the buffer, so keep looking
          if (_r2Done)
          {
            //If this happens that means that there was a problem. This means that r2 has
            //fully given us all of the stream and we haven't found the boundary.
            handleExceptions(new IllegalMultiPartMIMEFormatException("Malformed multipart mime request. No boundary found!"));
            return true; //We are in an unusable state so we return here.
          }
          _rh.request(1);
          return true;
        }
      }
      return false;
    }

    private boolean checkForSufficientBufferSize()
    {
      //We buffer forward a bit if we have is less then the finishing boundary size.
      //This is the minimum amount of data we need in the buffer before we can go forward.
      if (_byteBuffer.size() < _finishingBoundaryBytes.size())
      {
        //If this happens and r2 has not notified us that we are done, then this is a problem. This means that
        //r2 has already notified that we are done and we didn't see the finishing boundary earlier.
        //This should never happen.
        if (_r2Done)
        {
          //Notify the reader of the issue.
          handleExceptions(new IllegalMultiPartMIMEFormatException("Malformed multipart mime request. Finishing boundary missing!"));
          return true; //We are in an unusable state so we return here.
        }

        //Otherwise we need to read in some more data.
        _rh.request(1);
        return true;
      }

      return false;
    }

    private void performPartReading()
    {
      //READING_PARTS represents normal part reading operation and is where most of the time will be spent.
      //At this point in time in our logic this is guaranteed to be in this state.
      assert (_multiPartReaderState == MultiPartReaderState.READING_PARTS);

      //The goal of the logic here is the following:
      //1. If the buffer does not start with the boundary, then we fully consume as much of the buffer as possible.
      //We notify clients of as much data we can drain. Note that in such a case, even if the buffer does not start with
      //the boundary, it could still contain the boundary. In such a case we read up until the boundary. In this situation
      //the bytes read would be the last bits of data they need for the current part. Subsequent invocations of
      //requestPartData() would then lead to the buffer starting with the boundary.
      //
      //2. Otherwise if the buffer does start with boundary then we wrap up the previous part and
      //begin the new one.

      //Another invariant to note is that the result of this logic below will result in ONE of the
      //following (assuming there are no error conditions):
      //1. onPartDataAvailable()
      //OR
      //2. OnAbandoned() on SinglePartCallback followed by onNewPart() on MultiPartCallback
      //OR
      //3. OnFinished() on SinglePartCallback followed by onNewPart() on MultiPartCallback
      //OR
      //4. OnAbandoned() on SinglePartCallback followed by onFinished() on MultiPartCallback
      //OR
      //5. OnFinished() on SinglePartCallback followed by onFinished() on MultiPartCallback
      //
      //Note that onPartDataAvailable() and onNewPart() are never called one after another in the logic
      //below because upon invocation of these callbacks, clients may come back to us immediately
      //and it can potentially lead to very confusing states. Furthermore its also more intuitive
      //to answer each client's request with only one callback. This also allows us to use the iterative
      //callback invocation technique and return at a location in the code that is different then the original
      //invocation location.

      final int boundaryIndex;
      final int boundarySize;
      if (_firstBoundaryEvaluated == false)
      {
        //Immediately after the preamble, i.e the first part we are seeing
        boundaryIndex = Collections.indexOfSubList(_byteBuffer, _firstBoundaryBytes);
        boundarySize = _firstBoundaryBytes.size();
      }
      else
      {
        boundaryIndex = Collections.indexOfSubList(_byteBuffer, _normalBoundaryBytes);
        boundarySize = _normalBoundaryBytes.size();
      }

      //We return no matter what anyway, so no need to check to see if we should return for either of these.
      if (boundaryIndex != 0)
      {
        processBufferStartingWithoutBoundary(boundaryIndex);
      }
      else
      {
        processBufferStartingWithBoundary(boundarySize);
      }
    }

    private void processBufferStartingWithoutBoundary(final int boundaryIndex)
    {
      //We only proceed forward if there is a reader ready. By ready we mean that:
      //1. They are ready to receive requested data on their onPartDataAvailable() callback, meaning
      //REQUESTED_DATA.
      //or
      //2. They have requested an abort and are waiting for it to finish, meaning REQUESTED_ABORT.
      //
      //If the current single part reader is not ready, then we just return and move on (we already read into the buffer)
      //since the single part reader can then drive the flow of future data.
      //
      //It is further important to note that in the current implementation, the reader will ALWAYS be ready at this point in time.
      //This is because we strictly allow only our clients to push us forward. This means they must be in a ready state
      //when all of this logic is executed.
      //
      //If we need more bytes to fulfill their request we call _rh.request(1), but this only happens if the reader
      //is ready to begin with (since the reader drove us forward and refreshed this logic).
      //
      //Formally, here is why we don't do _rh.request(2)...i.e _rh.request(n>1):
      //A. If we did this, the first onDataAvailable() invoked by R2 would potentially satisfy a client's
      //request. The second onDataAvailable() invoked by R2 would then just write data into the local buffer. However
      //now we have to distinguish on whether or not the client drove us forward by refreshing us or our desire for more data
      //drove us forward. This leads to more complication and also performs reading of data that we don't need yet.
      //
      //B. Multiple threads could call the logic here concurrently thereby violating the guarantee we get that
      //the logic here is only run by one thread concurrently. For example:
      //If we did a _rh.request(2), then the first invocation of onDataAvailable() would satisfy a
      //client's request. The client could then drive us forward again by invoking onPartDataAvailable()
      //to refresh the logic. However at this time the answer to our second _rh.request() could also come in
      //thereby causing multiple threads to operate in an area where there is no synchronization.

      //Note that _currentSinglePartMIMEReader is guaranteed to be non-null at this point.
      final SingleReaderState currentState = _currentSinglePartMIMEReader._singleReaderState;

      //Assert on our invariant described above.
      assert (currentState == SingleReaderState.REQUESTED_DATA || currentState == SingleReaderState.REQUESTED_ABORT);

      //We know the buffer doesn't begin with the boundary, but we can take different action if there a boundary
      //exists in the buffer. This way we can consume the maximum amount.
      //If the boundary exists in the buffer we know we can read right up until it begins.
      //If it doesn't the maximum we can read out is limited (since we don't want to consume possible
      //future boundary data).
      if (boundaryIndex == -1)
      {
        //Boundary doesn't exist here, let's drain what we can.
        //Note that we can't fully drain the buffer because the end of the buffer may include the partial
        //beginning of the next boundary.
        final int amountToLeaveBehind = _normalBoundaryBytes.size() - 1;
        final List<Byte> useableBytes = _byteBuffer.subList(0, _byteBuffer.size() - amountToLeaveBehind);

        //Make a copy of what we need leaving the old list to be GC'd
        _byteBuffer = new ArrayList<Byte>(_byteBuffer.subList(_byteBuffer.size() - amountToLeaveBehind, _byteBuffer.size()));

        if (currentState == SingleReaderState.REQUESTED_DATA)
        {
          //Grab a copy of the data before we change the client state.
          final ByteString clientData = ByteString.copy(ArrayUtils.toPrimitive(useableBytes.toArray(new Byte[0])));

          //We must set this before we provide the data. Otherwise if the client immediately decides to requestPartData()
          //they will see an exception because we are still in REQUESTED_DATA. Technically they shouldn't do this
          //since everything is event driven, but we still maintain caution.
          _currentSinglePartMIMEReader._singleReaderState = SingleReaderState.CALLBACK_BOUND_AND_READY;

          //This effectively does:
          //_currentSinglePartMIMEReader._callback.onPartDataAvailable(clientData);
          final Callable<Void> onPartDataAvailableInvocation =
              new MimeReaderCallables.onPartDataCallable(_currentSinglePartMIMEReader._callback, clientData);

          //Queue up this operation
          _callbackQueue.add(onPartDataAvailableInvocation);

          //If the while loop before us is in progress, we just return
          if (_callbackInProgress)
          {
            //We return to unwind the stack. Any queued elements will be taken care of the by the while loop
            //before us.
            return;
          }
          else
          {
            processAndInvokeCallableQueue();
            //If invoking the callables resulting in things stopping, we will return anyway.
          }

          //The client single part reader can then drive forward themselves.
        }
        else
        {
          //This is an abort operation, so we need to drop the bytes and keep moving forward.
          //Note that we don't have a client to drive us forward so we do it ourselves.
          final Callable<Void> recursiveCallable = new MimeReaderCallables.recursiveCallable(this);

          //Queue up this operation
          _callbackQueue.add(recursiveCallable);

          //If the while loop before us is in progress, we just return
          if (_callbackInProgress)
          {
            //We return to unwind the stack. Any queued elements will be taken care of the by the while loop
            //before us.
            return;
          }
          else
          {
            processAndInvokeCallableQueue();
            //If invoking the callables resulting in things stopping, we will return anyway.
          }
          //No need to explicitly return here.
        }
      }
      else
      {
        //Boundary is in buffer. Could be normal boundary or it could be finishing boundary.
        final List<Byte> useableBytes = _byteBuffer.subList(0, boundaryIndex);
        //Make a copy of what we need leaving the old list to be GC'd.
        _byteBuffer = new ArrayList<Byte>(_byteBuffer.subList(boundaryIndex, _byteBuffer.size()));

        if (currentState == SingleReaderState.REQUESTED_DATA)
        {
          //Grab a copy of the data beforehand.
          final ByteString clientData = ByteString.copy(ArrayUtils.toPrimitive(useableBytes.toArray(new Byte[0])));

          //We must set this before we provide the data.
          _currentSinglePartMIMEReader._singleReaderState = SingleReaderState.CALLBACK_BOUND_AND_READY;

          //This effectively does:
          //_currentSinglePartMIMEReader._callback.onPartDataAvailable(clientData);
          final Callable<Void> onPartDataAvailableInvocation =
              new MimeReaderCallables.onPartDataCallable(_currentSinglePartMIMEReader._callback, clientData);

          //Queue up this operation
          _callbackQueue.add(onPartDataAvailableInvocation);

          //If the while loop before us is in progress, we just return
          if (_callbackInProgress)
          {
            //We return to unwind the stack. Any queued elements will be taken care of the by the while loop
            //before us.
            return;
          }
          else
          {
            processAndInvokeCallableQueue();
            //If invoking the callables resulting in things stopping, we will return anyway.
          }
        }
        else
        {
          //drop the bytes
          final Callable<Void> recursiveCallable = new MimeReaderCallables.recursiveCallable(this);

          //Queue up this operation
          _callbackQueue.add(recursiveCallable);

          //If the while loop before us is in progress, we just return
          if (_callbackInProgress)
          {
            //We return to unwind the stack. Any queued elements will be taken care of the by the while loop
            //before us.
            return;
          }
          else
          {
            processAndInvokeCallableQueue();
            //If invoking the callables resulting in things stopping, we will return anyway.
          }
          //No need to return explicitly from here.
        }
        //This part is finished. Subsequently when the client asks for more data our logic
        //will now see that the buffer begins with the boundary. This will finish up this part
        //and then make a new part.
      }
    }

    private void processBufferStartingWithBoundary(final int boundarySize)
    {
      //The beginning of the buffer contains a boundary. Finish of the current part first.
      //If performing this results in an exception then we can't continue so we return.
      if (finishCurrentPart())
      {
        return;
      }

      //Before continuing verify that this isn't the final boundary in front of us.
      final int lastBoundaryLookup = Collections.indexOfSubList(_byteBuffer, _finishingBoundaryBytes);
      if (lastBoundaryLookup == 0)
      {
        _multiPartReaderState = MultiPartReaderState.READING_EPILOGUE;
        //If r2 has already notified we are done, we can wrap up. Note that there still may be bytes
        //sitting in our byteBuffer that haven't been consumed. These bytes must be the epilogue
        //bytes so we can safely ignore them.
        if (_r2Done)
        {
          _multiPartReaderState = MultiPartReaderState.FINISHED;
          //There is no need to use our iterative technique to call this callback because a
          //client cannot possibly invoke us again.
          try
          {
            //This can throw so we need to notify the client that their APIs threw an exception when we invoked them.
            MultiPartMIMEReader.this._clientCallback.onFinished();
          }
          catch (RuntimeException clientCallbackException)
          {
            handleExceptions(clientCallbackException);
          }
          finally
          {
            return; //Regardless of whether or not the onFinished() threw, we're done so we must return here.
          }
        }
        //Keep on reading bytes and dropping them.
        _rh.request(1);
        return;
      }

      processNewPart(boundarySize);
    }

    private boolean finishCurrentPart()
    {
      //Close the current single part reader (except if this is the first boundary)
      if (_currentSinglePartMIMEReader != null)
      {
        if (_currentSinglePartMIMEReader._singleReaderState == SingleReaderState.REQUESTED_ABORT)
        {
          //If they cared to be notified of the abandonment.
          if (_currentSinglePartMIMEReader._callback != null)
          {
            //We need to prevent the client from asking for more data because they are done.
            _currentSinglePartMIMEReader._singleReaderState = SingleReaderState.FINISHED;

            //Note we do not need to use our iterative invocation technique here because
            //the client can't request more data.
            //Also it is important to note that we CANNOT use the iterative technique.
            //This is because the iterative technique will NOT return back here (to this line of
            //code) which does NOT guarantee us proceeding forward from here.
            //We need to proceed forward from here to move onto the next part.
            try
            {
              _currentSinglePartMIMEReader._callback.onAbandoned();
            }
            catch (RuntimeException clientCallbackException)
            {
              //This could throw so handle appropriately.
              handleExceptions(clientCallbackException);
              return true; //We return since we are in an unusable state.
            }
          } //else no notification will happen since there was no callback registered.
        }
        else
        {
          //This was a part that cared about its data. Let's finish him up.

          //We need to prevent the client from asking for more data because they are done.
          _currentSinglePartMIMEReader._singleReaderState = SingleReaderState.FINISHED;

          //Note we do not need to use our iterative invocation technique here because
          //the client can't request more data.
          //Also it is important to note that we CANNOT use the iterative technique.
          //This is because the iterative technique will NOT return back here (to this line of
          //code) which does NOT guarantee us proceeding forward from here.
          //We need to proceed forward from here to move onto the next part.
          try
          {
            _currentSinglePartMIMEReader._callback.onFinished();
          }
          catch (RuntimeException clientCallbackException)
          {
            //This could throw so handle appropriately.
            handleExceptions(clientCallbackException);
            return true; //We return since we are in an unusable state.
          }
        }
        _currentSinglePartMIMEReader = null;
      }

      return false;
    }

    private void processNewPart(final int boundarySize)
    {
      //Now read until we have all the headers. Headers may or may not exist. According to the RFC:
      //If the headers do not exist, we will see two CRLFs one after another.
      //If at least one header does exist, we will see the headers followed by two CRLFs
      //Essentially we are looking for the first occurrence of two CRLFs after we see the boundary.

      //We need to make sure we can look ahead a bit here first. The minimum size of the buffer must be
      //the size of the normal boundary plus consecutive CRLFs. This would be the bare minimum as it conveys
      //empty headers.
      if ((boundarySize + MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST.size()) > _byteBuffer.size())
      {
        if (_r2Done)
        {
          //If r2 has already notified we are done, then this is a problem. This means that
          //we have the remainder of the stream in memory and we see a non-finishing boundary that terminates
          //immediately without a CRLF_BYTES. This is a sign of a stream that was prematurely terminated.
          //MultiPartMIMEReader.this._clientCallback.onStreamError();
          handleExceptions(new IllegalMultiPartMIMEFormatException("Malformed multipart mime request. Premature"
              + " termination of multipart mime body due to a boundary without a subsequent consecutive CRLF."));
          return; //Unusable state, so return.
        }
        _rh.request(1);
        return;
      }

      //Now we will determine the existence of headers.
      //In order to do this we construct a window to look into. We will look inside of the buffer starting at the
      //end of the boundary until the end of the buffer.
      final List<Byte> possibleHeaderArea = _byteBuffer.subList(boundarySize, _byteBuffer.size());

      //Find the two consecutive CRLFs.
      final int headerEnding = Collections.indexOfSubList(possibleHeaderArea, MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST);
      if (headerEnding == -1)
      {
        if (_r2Done)
        {
          //If r2 has already notified us we are done, then this is a problem. This means that we saw a
          //a boundary followed by a potential header area. This header area does not contain
          //two consecutive CRLF_BYTES characters. This is a malformed stream.
          handleExceptions(new IllegalMultiPartMIMEFormatException("Malformed multipart mime request. Premature "
              + "termination of headers within a part."));
          return;//Unusable state, so return.
        }
        //We need more data since the current buffer doesn't contain the CRLFs.
        _rh.request(1);
        return;
      }

      //At this point, headerEnding represents the location of the first occurrence of consecutive CRLFs.
      //It is important to note that it is possible for a malformed stream to not end its headers with consecutive
      //CRLFs. In such a case, everything up until the first occurrence of the consecutive CRLFs will be considered
      //part of the header area.

      //Let's make a window into the header area. Note that we need to include the trailing consecutive CRLF bytes
      //because we need to verify if the header area is empty, meaning it contains only consecutive CRLF bytes.
      final List<Byte> headerByteSubList =
          possibleHeaderArea.subList(0, headerEnding + MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST.size());

      final Map<String, String> headers;
      if (headerByteSubList.equals(MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST))
      {
        //The region of bytes after the boundary is composed of two CRLFs. Therefore we have no headers.
        headers = Collections.emptyMap();
      }
      else
      {
        headers = new HashMap<String, String>();

        //We have headers, lets read them in - we search using a sliding window.

        //Currently our buffer is sitting just past the end of the boundary. Beyond this boundary
        //there should be a CRLF followed by the first header. We will verify that this is indeed a CRLF
        //and then we will skip it below.
        final List<Byte> leadingBytes = headerByteSubList.subList(0, MultiPartMIMEUtils.CRLF_BYTE_LIST.size());
        if (!leadingBytes.equals(MultiPartMIMEUtils.CRLF_BYTE_LIST))
        {
          handleExceptions(new IllegalMultiPartMIMEFormatException(
              "Malformed multipart mime request. Headers are improperly constructed."));
          return; //Unusable state, so return.
        }

        //The sliding-window-header-split technique here works because we are essentially splitting the buffer
        //by looking at occurrences of CRLF bytes. This is analogous to splitting a String in Java but instead
        //we are splitting a byte array.

        //We start at an offset of i and currentHeaderStart because we need to skip the first CRLF.
        int currentHeaderStart = MultiPartMIMEUtils.CRLF_BYTE_LIST.size();
        final StringBuffer runningFoldedHeader = new StringBuffer(); //For folded headers. See below for details.

        //Note that the end of the buffer we are sliding through is composed of two consecutive CRLFs.
        //Our sliding window algorithm here will NOT evaluate the very last CRLF bytes (which would otherwise
        //erroneously result in an empty header).
        for (int i = MultiPartMIMEUtils.CRLF_BYTE_LIST.size(); i < headerByteSubList.size() - MultiPartMIMEUtils.CRLF_BYTE_LIST.size(); i++)
        {
          final List<Byte> currentWindow = headerByteSubList.subList(i, i + MultiPartMIMEUtils.CRLF_BYTE_LIST.size());
          if (currentWindow.equals(MultiPartMIMEUtils.CRLF_BYTE_LIST))
          {
            final List<Byte> currentHeaderBytes = headerByteSubList.subList(currentHeaderStart, i);
            //At this point we MAY have found the end of a header because the current window is a CRLF.
            //This could POTENTIALLY mean that from currentHeaderStart until i is a header.

            //However before we can reach this conclusion we must check for header folding. Header folding is described
            //in RFC 822 which states that headers may take up multiple lines (therefore delimited by CRLFs).
            //This rule only holds true if there is exactly one CRLF followed by atleast one LWSP (linear white space).
            //A LWSP can be composed of multiple spaces, tabs or newlines. However most implementations only use
            //spaces or tabs. Therefore our reading of folded headers will support only CRLFs followed by atleast one
            //space or tab.
            //Furthermore this syntax is deprecated so there is no need for us to formally support the RFC here as
            //long as we cover interoperability with major libraries.

            //Therefore we have two options here:
            //1. If the character in front of us IS a tab or a white space, we must consider this the first part of a
            //multi line header value. In such a case we have to keep going forward and append the current header value.
            //2. Otherwise the character in front of us is NOT a tab or a white space. We can then consider the current
            //header bytes to compose a header that fits on a single line.

            final byte[] headerBytes = ArrayUtils.toPrimitive(currentHeaderBytes.toArray(new Byte[0]));
            String header = new String(headerBytes);

            if (headerByteSubList.get(i + MultiPartMIMEUtils.CRLF_BYTE_LIST.size()) == MultiPartMIMEUtils.SPACE_BYTE
                || headerByteSubList.get(i + MultiPartMIMEUtils.CRLF_BYTE_LIST.size()) == MultiPartMIMEUtils.TAB_BYTE)
            {
              //Append the running concatenation of the folded header. We need to preserve the original header so
              //we also include the CRLF. The subsequent LWSP(s) will be also preserved because we don't trim here.
              runningFoldedHeader.append(header + MultiPartMIMEUtils.CRLF_STRING);
            }
            else
            {
              //This is a single line header OR we arrived at the last line of a folded header.
              if (runningFoldedHeader.length() != 0)
              {
                runningFoldedHeader.append(header);
                header = runningFoldedHeader.toString();
                runningFoldedHeader.setLength(0); //Clear the buffer for future folded headers in this part
              }

              //Note that according to the RFC that header values may contain semi colons but header names may not.
              //Therefore it is acceptable to split on semicolon here and derive the header name from 0 -> semicolonIndex.
              final int colonIndex = header.indexOf(":");
              if (colonIndex == -1)
              {
                handleExceptions(new IllegalMultiPartMIMEFormatException(
                    "Malformed multipart mime request. Individual headers are improperly formatted."));
                return; //Unusable state, so return.
              }
              headers.put(header.substring(0, colonIndex).trim(),
                  header.substring(colonIndex + 1, header.length()).trim());
            }
            currentHeaderStart = i + MultiPartMIMEUtils.CRLF_BYTE_LIST.size();
          }
        }
      }

      //At this point we have actual part data starting from headerEnding going forward
      //which means we can dump everything else beforehand. We need to skip past the trailing consecutive CRLFs.
      final int consumedDataIndex = boundarySize + headerEnding + MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST.size();
      //Make a copy of what we need leaving the old list to be GC'd.
      _byteBuffer = new ArrayList<Byte>(_byteBuffer.subList(consumedDataIndex, _byteBuffer.size()));

      //Notify the callback that we have a new part
      _currentSinglePartMIMEReader = new SinglePartMIMEReader(headers);

      //_clientCallback.onNewPart(_currentSinglePartMIMEReader);
      final Callable<Void> onNewPartInvocation =
          new MimeReaderCallables.onNewPartCallable(_clientCallback, _currentSinglePartMIMEReader);

      //Queue up this operation
      _callbackQueue.add(onNewPartInvocation);

      //We can now switch to absorbing the normal boundary
      _firstBoundaryEvaluated = true;

      //If the while loop before us is in progress, we just return
      if (_callbackInProgress)
      {
        //We return to unwind the stack. Any queued elements will be taken care of the by the while loop
        //before us.
        return;
      }
      else
      {
        processAndInvokeCallableQueue();
        //No need to explicitly return here even if this invocation results in an exception.
      }
    }

    void handleExceptions(final Throwable throwable)
    {
      //All exceptions caught here should put the reader in a non-usable state. Continuing from this point forward
      //is not feasible.
      //We also will cancel here and have R2 read and drop all bytes on the floor. Otherwise we are obliged to read
      //and drop all bytes on the floor. It does not make any sense to enter this obligation when we are in
      //a non-usable state.
      //Exceptions here are indicative that there was malformed data provided to the MultiPartMIMEReader
      //OR that the client APIs threw exceptions themselves when their callbacks were invoked.
      //We will also invoke the appropriate callbacks here indicating there is an exception while reading.
      //It is the responsibility of the consumer of this library to catch these exceptions and return 4xx.

      _rh.cancel();
      _multiPartReaderState = MultiPartReaderState.FINISHED;
      try
      {
        _clientCallback.onStreamError(throwable);
      }
      catch (RuntimeException runtimeException)
      {
        //Swallow. What more can we do here?
      }
      if (_currentSinglePartMIMEReader != null)
      {
        _currentSinglePartMIMEReader._singleReaderState = SingleReaderState.FINISHED;

        try
        {
          _currentSinglePartMIMEReader._callback.onStreamError(throwable);
        }
        catch (RuntimeException runtimeException)
        {
          //Swallow. What more can we do here?
        }
      }
    }

    private R2MultiPartMIMEReader(final String boundary)
    {
      _firstBoundary = "--" + boundary;
      _normalBoundary = MultiPartMIMEUtils.CRLF_STRING + "--" + boundary;
      _finishingBoundary = _normalBoundary + "--";

      for (final byte b : _firstBoundary.getBytes())
      {
        _firstBoundaryBytes.add(b);
      }

      for (final byte b : _normalBoundary.getBytes())
      {
        _normalBoundaryBytes.add(b);
      }

      for (final byte b : _finishingBoundary.getBytes())
      {
        _finishingBoundaryBytes.add(b);
      }
    }
  }

  //Package private for testing
  enum MultiPartReaderState
  {
    CREATED, //At the very beginning. Before the callback is even bound.
    CALLBACK_BOUND_AND_READING_PREAMBLE, //Callback is bound and we have started to read the preamble in.
    READING_PARTS, //Normal operation. Most time should be spent in this state.
    READING_EPILOGUE, //Epilogue is being read.
    ABANDONING, //Client asked for an complete abandonment.
    FINISHED //The reader is no longer usable.
  }

  /**
   * Create a MultiPartMIMEReader by acquiring the {@link com.linkedin.r2.message.streaming.EntityStream} from the
   * provided {@link com.linkedin.r2.message.rest.StreamRequest}.
   *
   * Interacting with the MultiPartMIMEReader will happen through callbacks invoked on the provided
   * {@link com.linkedin.multipart.MultiPartMIMEReaderCallback}.
   *
   * @param request the request containing the {@link com.linkedin.r2.message.streaming.EntityStream} to read from.
   * @param clientCallback the callback to invoke in order to drive the client forward for reading part data.
   * @return the newly created MultiPartMIMEReader
   * @throws IllegalMultiPartMIMEFormatException if the request is in any way not a valid multipart/mime request.
   */
  public static MultiPartMIMEReader createAndAcquireStream(final StreamRequest request,
      final MultiPartMIMEReaderCallback clientCallback) throws IllegalMultiPartMIMEFormatException
  {
    return new MultiPartMIMEReader(request, clientCallback);
  }

  /**
   * Create a MultiPartMIMEReader by acquiring the {@link com.linkedin.r2.message.streaming.EntityStream} from the
   * provided {@link com.linkedin.r2.message.rest.StreamResponse}.
   *
   * Interacting with the MultiPartMIMEReader will happen through callbacks invoked on the provided
   * {@link com.linkedin.multipart.MultiPartMIMEReaderCallback}.
   *
   * @param response the response containing the {@link com.linkedin.r2.message.streaming.EntityStream} to read from.
   * @param clientCallback the callback to invoke in order to drive the client forward for reading part data.
   * @return the newly created MultiPartMIMEReader
   * @throws IllegalMultiPartMIMEFormatException if the response is in any way not a valid multipart/mime response.
   */
  public static MultiPartMIMEReader createAndAcquireStream(final StreamResponse response,
      final MultiPartMIMEReaderCallback clientCallback) throws IllegalMultiPartMIMEFormatException
  {
    return new MultiPartMIMEReader(response, clientCallback);
  }

  /**
   * Create a MultiPartMIMEReader by acquiring the {@link com.linkedin.r2.message.streaming.EntityStream} from the
   * provided {@link com.linkedin.r2.message.rest.StreamRequest}.
   *
   * Interacting with the MultiPartMIMEReader will require subsequent registration using a
   * {@link com.linkedin.multipart.MultiPartMIMEReaderCallback}.
   *
   * @param request the request containing the {@link com.linkedin.r2.message.streaming.EntityStream} to read from.
   * @return the newly created MultiPartMIMEReader
   * @throws IllegalMultiPartMIMEFormatException if the request is in any way not a valid multipart/mime request.
   */
  public static MultiPartMIMEReader createAndAcquireStream(final StreamRequest request)
      throws IllegalMultiPartMIMEFormatException
  {
    return new MultiPartMIMEReader(request, null);
  }

  /**
   * Create a MultiPartMIMEReader by acquiring the {@link com.linkedin.r2.message.streaming.EntityStream} from the
   * provided {@link com.linkedin.r2.message.rest.StreamResponse}.
   *
   * Interacting with the MultiPartMIMEReader will require subsequent registration using a
   * {@link com.linkedin.multipart.MultiPartMIMEReaderCallback}.
   *
   * @param response the response containing the {@link com.linkedin.r2.message.streaming.EntityStream} to read from.
   * @return the newly created MultiPartMIMEReader
   * @throws IllegalMultiPartMIMEFormatException if the response is in any way not a valid multipart/mime response.
   */
  public static MultiPartMIMEReader createAndAcquireStream(final StreamResponse response)
      throws IllegalMultiPartMIMEFormatException
  {
    return new MultiPartMIMEReader(response, null);
  }

  private MultiPartMIMEReader(final StreamRequest request, final MultiPartMIMEReaderCallback clientCallback)
      throws IllegalMultiPartMIMEFormatException
  {
    final String contentTypeHeaderValue = request.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER);
    if (contentTypeHeaderValue == null)
    {
      throw new IllegalMultiPartMIMEFormatException(
          "Malformed multipart mime request. No Content-Type header in this request");
    }

    _reader = new R2MultiPartMIMEReader(MultiPartMIMEUtils.extractBoundary(contentTypeHeaderValue));
    _entityStream = request.getEntityStream();
    _multiPartReaderState = MultiPartReaderState.CREATED;
    if (clientCallback != null)
    {
      _clientCallback = clientCallback;
      _entityStream.setReader(_reader);
    }
  }

  private MultiPartMIMEReader(StreamResponse response, MultiPartMIMEReaderCallback clientCallback)
      throws IllegalMultiPartMIMEFormatException
  {
    final String contentTypeHeaderValue = response.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER);
    if (contentTypeHeaderValue == null)
    {
      throw new IllegalMultiPartMIMEFormatException(
          "Malformed multipart mime request. No Content-Type header in this response");
    }

    _reader = new R2MultiPartMIMEReader(MultiPartMIMEUtils.extractBoundary(contentTypeHeaderValue));
    _entityStream = response.getEntityStream();
    _multiPartReaderState = MultiPartReaderState.CREATED;
    if (clientCallback != null)
    {
      _clientCallback = clientCallback;
      _entityStream.setReader(_reader);
    }
  }

  /**
   * Indicates if all parts have been finished from this MultiPartMIMEReader.
   *
   * @return true if the reader is finished.
   */
  public boolean haveAllPartsFinished()
  {
    return _multiPartReaderState == MultiPartReaderState.FINISHED;
  }

  /**
   * Reads through and abandons the current new part and additionally the whole stream. This API can ONLY be used after
   * registration using a {@link com.linkedin.multipart.MultiPartMIMEReaderCallback}
   * and after an invocation on {@link MultiPartMIMEReaderCallback#onNewPart(com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader)}.
   *
   * The goal is for clients to at least see the first part before deciding to abandon all parts.
   *
   * As described, a valid {@link com.linkedin.multipart.MultiPartMIMEReaderCallback} is required to use this API.
   * Failure to do so will result in a {@link com.linkedin.multipart.exceptions.ReaderNotInitializedException}.
   *
   * This can ONLY be called if there is no part being actively read, meaning that
   * the current {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader} has not been initialized
   * with a {@link com.linkedin.multipart.SinglePartMIMEReaderCallback}. If this is violated we throw a
   * {@link com.linkedin.multipart.exceptions.StreamBusyException}.
   *
   * Once the stream is finished being abandoned, we call {@link MultiPartMIMEReaderCallback#onAbandoned()}.
   *
   * If the stream is finished, subsequent calls will throw {@link com.linkedin.multipart.exceptions.ReaderFinishedException}.
   *
   * Since this is async and we do not allow request queueing, repetitive calls will result in
   * {@link com.linkedin.multipart.exceptions.StreamBusyException}.
   */
  public void abandonAllParts()
  {
    if (_multiPartReaderState == MultiPartReaderState.CREATED)
    {
      throw new ReaderNotInitializedException("No callback registered with the reader. Unable to proceed.");
    }

    //We are already done or almost done.
    if (_multiPartReaderState == MultiPartReaderState.FINISHED
        || _multiPartReaderState == MultiPartReaderState.READING_EPILOGUE)
    {
      throw new ReaderFinishedException("The reader is finished therefore it cannot proceed.");
    }

    if (_multiPartReaderState == MultiPartReaderState.CALLBACK_BOUND_AND_READING_PREAMBLE)
    {
      throw new StreamBusyException("The reader is busy processing the preamble. Unable to proceed with abandonment. "
          + "Please only call abandonAllParts() upon invocation of onNewPart() on the client callback.");
    }

    if (_multiPartReaderState == MultiPartReaderState.ABANDONING)
    {
      throw new StreamBusyException("Reader already busy abandoning.");
    }

    //At this point we know we are in READING_PARTS which is the desired state.

    //We require that there exist a valid, non-null SinglePartMIMEReader before we continue since the contract is that
    //the top level callback can only abandon upon witnessing onNewPart().

    //Note that there is a small window of opportunity where a client registers the callback and invokes
    //abandonAllParts() after the reader has read the preamble in but before the reader has invoked onNewPart().
    //This can happen, but so can a client invoking us concurrently which is forbidden. Therefore we will not check
    //for such a race.

    //At this stage we know for a fact that onNewPart() has been invoked on the reader callback. Just make sure its
    //at the beginning of a new part before we continue allowing the abort.
    if (_currentSinglePartMIMEReader._singleReaderState != SingleReaderState.CREATED)
    {
      throw new StreamBusyException("Unable to abandon all parts due to current SinglePartMIMEReader in use.");
    }

    _currentSinglePartMIMEReader._singleReaderState = SingleReaderState.FINISHED;
    _multiPartReaderState = MultiPartReaderState.ABANDONING;

    _reader.processEventAndInvokeClient();
  }

  /**
   * Register to read using this MultiPartMIMEReader. This can ONLY be called if there is no part being actively
   * read meaning that the current {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader}
   * has not had a callback registered with it. Violation of this will throw a {@link com.linkedin.multipart.exceptions.StreamBusyException}.
   *
   * This can even be set if no parts in the stream have actually been consumed, i.e after the very first invocation of
   * {@link MultiPartMIMEReaderCallback#onNewPart(com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader)}.
   *
   * This API is used by the {@link com.linkedin.multipart.MultiPartMIMEWriter} to chain and use as a data source. Most
   * developers will not have a need to use this API directly.
   *
   * @param clientCallback the {@link com.linkedin.multipart.MultiPartMIMEReaderCallback} which will be invoked upon
   *                       to read this multipart mime body.
   */
  public void registerReaderCallback(final MultiPartMIMEReaderCallback clientCallback)
      throws ReaderFinishedException, StreamBusyException
  {
    //First we throw exceptions for all _reader states where it is incorrect to transfer callbacks.
    //We have to handle all the individual incorrect states one by one so that we can that we can throw
    //fine grain exceptions.

    if (_multiPartReaderState == MultiPartReaderState.FINISHED
        || _multiPartReaderState == MultiPartReaderState.READING_EPILOGUE)
    {
      throw new ReaderFinishedException("Unable to register a callback. This reader has already finished reading.");
    }

    if (_multiPartReaderState == MultiPartReaderState.CALLBACK_BOUND_AND_READING_PREAMBLE)
    {
      //This would happen if a client registers a callback multiple times
      //immediately.
      throw new StreamBusyException(
          "Reader is busy reading in the preamble. Unable to register the callback at this time.");
    }

    if (_multiPartReaderState == MultiPartReaderState.ABANDONING)
    {
      throw new StreamBusyException(
          "Reader is busy performing a complete abandonment. Unable to register the callback.");
    }

    //At this point we know that _reader is in CREATED or READING_PARTS

    //Now we verify that single part reader is in the correct state.
    //The first time the callback is registered, _currentSinglePartMIMEReader will be null which is fine.
    //Subsequent calls will verify that the _currentSinglePartMIMEReader is in the desired state.
    if (_currentSinglePartMIMEReader != null
        && _currentSinglePartMIMEReader._singleReaderState != SingleReaderState.CREATED)
    {
      throw new StreamBusyException(
          "Unable to register callback on the reader since there is currently a SinglePartMIMEReader in use, meaning "
              + "that it was registered with a SinglePartMIMEReaderCallback."); //Can't transition at this point in time
    }

    if (_clientCallback == null)
    {
      //This is the first time it's being set
      _multiPartReaderState = MultiPartReaderState.CALLBACK_BOUND_AND_READING_PREAMBLE;
      _clientCallback = clientCallback;
      _entityStream.setReader(_reader);
      return;
    }
    else
    {
      //This is just a transfer to a new callback.
      _clientCallback = clientCallback;
    }

    //Start off the new client callback. If there was already a _currentSinglePartMIMEReader, we get them started off
    //on that. Otherwise if this is the first time then we can just return from here and let the fact we setReader()
    //above drive the first invocation of onNewPart().
    //Note that it is not possible for _currentSinglePartMIMEReader to be null at ANY time except the very first time
    //registerReaderCallback() is invoked.

    if (_currentSinglePartMIMEReader != null)
    {
      //Also note that if the client is really abusive it is possible for them
      //to call registerReaderCallback() over and over again which would lead to a stack overflow.
      //However this is clearly a client bug so we will not account for it here.

      try
      {
        _clientCallback.onNewPart(_currentSinglePartMIMEReader);
      }
      catch (RuntimeException exception)
      {
        //The callback could throw here, at which point we let them know what they just did and shut things down.
        _reader.handleExceptions(exception);
      }
    }
  }

  R2MultiPartMIMEReader getR2MultiPartMIMEReader()
  {
    return _reader;
  }

  /* Package private and used for testing */
  void setState(final MultiPartReaderState multiPartReaderState)
  {
    _multiPartReaderState = multiPartReaderState;
  }

  /* Package private and used for testing */
  void setCurrentSinglePartMIMEReader(final SinglePartMIMEReader singlePartMIMEReader)
  {
    _currentSinglePartMIMEReader = singlePartMIMEReader;
  }

  /**
   * A reader to register with and walk through an individual multipart mime body.
   *
   * When a new SinglePartMIMEReader is available, clients will be invoked on
   * {@link MultiPartMIMEReaderCallback#onNewPart(com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader)}.
   * At this time, clients should register an instance of {@link com.linkedin.multipart.SinglePartMIMEReaderCallback}
   * and then call {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader#requestPartData()}
   * to start the flow of data.
   */
  public final class SinglePartMIMEReader implements MultiPartMIMEDataSource
  {
    private final Map<String, String> _headers;
    private volatile SinglePartMIMEReaderCallback _callback = null;
    private final R2MultiPartMIMEReader _r2MultiPartMIMEReader;
    private volatile SingleReaderState _singleReaderState = SingleReaderState.CREATED;

    /**
     * Only MultiPartMIMEReader can ever create one of these.
     */
    SinglePartMIMEReader(Map<String, String> headers)
    {
      _r2MultiPartMIMEReader = MultiPartMIMEReader.this._reader;
      _headers = headers;
    }

    /**
     * This call registers a callback and commits to reading this part. This can only happen once per life of each
     * SinglePartMIMEReader. Subsequent attempts to modify this will throw
     * {@link com.linkedin.multipart.exceptions.PartBindException}.
     *
     * @param callback the callback to be invoked on in order to read data
     */
    public void registerReaderCallback(SinglePartMIMEReaderCallback callback)
    {
      //We don't have to check each and every state here. In order to reach any of those states the _callback must
      //have been not null to begin with, so we just check for that.
      if (_singleReaderState != SingleReaderState.CREATED)
      {
        throw new PartBindException("Callback already registered.");
      }
      _singleReaderState = SingleReaderState.CALLBACK_BOUND_AND_READY;
      _callback = callback;
    }

    /**
     * Returns the headers for this part. For parts that have no headers, this will return
     * {@link java.util.Collections#emptyMap()}
     *
     * @return
     */
    public Map<String, String> getHeaders()
    {
      return _headers;
    }

    /**
     * Read bytes from this part and notify the registered callback on
     * {@link SinglePartMIMEReaderCallback#onPartDataAvailable(com.linkedin.data.ByteString)}.
     *
     * Usage of this API requires registration using a {@link com.linkedin.multipart.SinglePartMIMEReaderCallback}.
     * Failure to do so will throw a {@link com.linkedin.multipart.exceptions.PartNotInitializedException}.
     *
     * If this part is fully consumed, meaning {@link SinglePartMIMEReaderCallback#onFinished()} has been called,
     * then any subsequent calls to requestPartData() will throw {@link com.linkedin.multipart.exceptions.PartFinishedException}.
     *
     * Since this is async and we do not allow request queueing, repetitive calls will result in
     * {@link com.linkedin.multipart.exceptions.StreamBusyException}.
     *
     * If the r2 reader is done, either through an error or a proper finish. Calls to requestPartData() will throw
     * {@link com.linkedin.multipart.exceptions.PartFinishedException}.
     */
    public void requestPartData()
    {
      verifyState();

      //Additionally, unlike abandonPartData(), requestPartData() can only be used if a callback is registered.
      if (_singleReaderState == SingleReaderState.CREATED)
      {
        throw new PartNotInitializedException("This SinglePartMIMEReader has not had a callback registered with it yet.");
      }

      //We know we are now at SingleReaderState.CALLBACK_BOUND_AND_READY
      _singleReaderState = SingleReaderState.REQUESTED_DATA;

      //We have updated our desire to be notified of data. Now we signal the reader to refresh itself and forcing it
      //to read from the internal buffer as much as possible. We do this by notifying it of an empty ByteString.
      _r2MultiPartMIMEReader.processEventAndInvokeClient();
    }

    /**
     * Abandons all bytes from this part and then notify the registered callback (if present) on
     * {@link SinglePartMIMEReaderCallback#onAbandoned()}.
     *
     * Usage of this API does NOT require registration using a {@link com.linkedin.multipart.SinglePartMIMEReaderCallback}.
     * If there is no callback registration then there is no notification provided upon completion of abandoning
     * this part.
     *
     * If this part is fully consumed, meaning {@link SinglePartMIMEReaderCallback#onFinished()} has been called,
     * then any subsequent calls to abandonPart() will throw {@link com.linkedin.multipart.exceptions.PartFinishedException}.
     *
     * Since this is async and we do not allow request queueing, repetitive calls will result in
     * {@link com.linkedin.multipart.exceptions.StreamBusyException}.
     *
     * If the r2 reader is done, either through an error or a proper finish. Calls to requestPartData() will throw
     * {@link com.linkedin.multipart.exceptions.PartFinishedException}.
     */
    public void abandonPart()
    {
      verifyState();

      //We know we are now at SingleReaderState.CALLBACK_BOUND_AND_READY
      _singleReaderState = SingleReaderState.REQUESTED_ABORT;

      //We have updated our desire to be aborted. Now we signal the reader to refresh itself and forcing it
      //to read from the internal buffer as much as possible. We do this by notifying it of an empty ByteString.
      _r2MultiPartMIMEReader.processEventAndInvokeClient();
    }

    //Package private for testing.
    void verifyState()
    {
      if (_singleReaderState == SingleReaderState.FINISHED)
      {
        throw new PartFinishedException("This SinglePartMIMEReader has already finished.");
      }

      if (_singleReaderState == SingleReaderState.REQUESTED_DATA)
      {
        throw new StreamBusyException("This SinglePartMIMEReader is currently busy fulfilling a call to requestPartData().");
      }

      if (_singleReaderState == SingleReaderState.REQUESTED_ABORT)
      {
        throw new StreamBusyException("This SinglePartMIMEReader is currently busy fulfilling a call to abandonPart().");
      }
    }

    /* Package private for testing */
    void setState(final SingleReaderState singleReaderState)
    {
      _singleReaderState = singleReaderState;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////
    // Chaining interface implementation.
    private volatile WriteHandle _writeHandle;

    @Override
    public void onInit(WriteHandle writeHandle)
    {
      //We have been informed that this part will be treated as a data source by the MultiPartMIMEWriter.
      //So we will prepare for this task by:
      //1. Storing the handle to write data to.
      //2. Creating a callback to register ourselves with.
      _writeHandle = writeHandle;
      SinglePartMIMEReaderCallback singlePartMIMEChainReaderCallback =
          new SinglePartMIMEChainReaderCallback(writeHandle, this, true);
      registerReaderCallback(singlePartMIMEChainReaderCallback);
    }

    @Override
    public void onWritePossible()
    {
      //When we are told to produce some data we will requestPartData() on ourselves which will
      //result in onPartDataAvailable() in SinglePartMIMEReaderDataSourceCallback(). The result of that will write
      //to the writeHandle which will write it further down stream.
      requestPartData();
    }

    @Override
    public void onAbort(Throwable e)
    {
      //Occurs when we send a part off to someone else and it gets aborted when they are told to write. In such a case
      //this indicates an error in reading.
      //Note that this could only occur if this was an individual part as a SinglePartMIMEReader specified to be sent out.
      //In this case the application developer can gracefully recover after being called onStreamError() on the
      //MultiPartMIMEReaderCallback.
      MultiPartMIMEReader.this._clientCallback.onStreamError(e);
    }

    @Override
    public Map<String, String> dataSourceHeaders()
    {
      return _headers;
    }
  }

  //Package private for testing
  enum SingleReaderState
  {
    CREATED, //Initial construction, no callback bound.
    CALLBACK_BOUND_AND_READY, //Callback has been bound, read to use APIs.
    REQUESTED_DATA, //Requested data, waiting to be notified.
    REQUESTED_ABORT, //Waiting for an abort to finish.
    FINISHED //This reader is done.
  }
}