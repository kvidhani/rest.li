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

import java.util.concurrent.Callable;


/**
 * These are all essentially function pointers to use in our iterative callback invocation scheme in MultiPartMIMEReader
 * to avoid stack overflows.
 *
 * @author Karim Vidhani
 */
final class MimeReaderCallables
{
  //SinglePartMIMEReaderCallback callable wrappers:
  static class onPartDataCallable implements Callable<Void>
  {
    private final SinglePartMIMEReaderCallback _singlePartMIMEReaderCallback;
    private final ByteString _data;

    @Override
    public Void call() throws Exception
    {
      _singlePartMIMEReaderCallback.onPartDataAvailable(_data);
      return null; //This is ignored.
    }

    onPartDataCallable(final SinglePartMIMEReaderCallback singlePartMIMEReaderCallback, final ByteString data)
    {
      _singlePartMIMEReaderCallback = singlePartMIMEReaderCallback;
      _data = data;
    }
  }

  //MultiPartMIMEReader callable wrappers:
  static class onNewPartCallable implements Callable<Void>
  {
    private final MultiPartMIMEReaderCallback _multiPartMIMEReaderCallback;
    private final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;

    @Override
    public Void call() throws Exception
    {
      _multiPartMIMEReaderCallback.onNewPart(_singlePartMIMEReader);
      return null; //This is ignored
    }

    onNewPartCallable(final MultiPartMIMEReaderCallback multiPartMIMEReaderCallback,
        final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader)
    {
      _multiPartMIMEReaderCallback = multiPartMIMEReaderCallback;
      _singlePartMIMEReader = singlePartMIMEReader;
    }
  }

  static class recursiveCallable implements Callable<Void>
  {
    private final MultiPartMIMEReader.R2MultiPartMIMEReader _r2MultiPartMIMEReader;

    @Override
    public Void call() throws Exception
    {
      _r2MultiPartMIMEReader.onDataAvailable(ByteString.empty());
      return null; //This is ignored
    }

    recursiveCallable(final MultiPartMIMEReader.R2MultiPartMIMEReader r2MultiPartMIMEReader)
    {
      _r2MultiPartMIMEReader = r2MultiPartMIMEReader;
    }
  }
}