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
import com.linkedin.r2.message.stream.entitystream.WriteHandle;

import java.io.IOException;


/**
 * Callback registered by the {@link com.linkedin.multipart.MultiPartMIMEWriter} to chain
 * a {@link com.linkedin.multipart.MultiPartMIMEDataSourceIterator} as a data source.
 *
 * @author Karim Vidhani
 */
final class MultiPartMIMEChainReaderCallback implements MultiPartMIMEDataSourceIteratorCallback
{
  private final WriteHandle _writeHandle;
  private MultiPartMIMEDataSource _currentDataSource;
  private final byte[] _normalEncapsulationBoundary;

  @Override
  public void onNewDataSource(final MultiPartMIMEDataSource multiPartMIMEDataSource)
  {
    multiPartMIMEDataSource.onInit(_writeHandle);
    _currentDataSource = multiPartMIMEDataSource;

    ByteString serializedBoundaryAndHeaders = null;
    try
    {
      serializedBoundaryAndHeaders =
          MultiPartMIMEUtils.serializeBoundaryAndHeaders(_normalEncapsulationBoundary, multiPartMIMEDataSource);
    }
    catch (IOException ioException)
    {
      onStreamError(ioException); //Should never happen
    }

    _writeHandle.write(serializedBoundaryAndHeaders);
    if (_writeHandle.remaining() > 0)
    {
      multiPartMIMEDataSource.onWritePossible();
    }
  }

  @Override
  public void onFinished()
  {
    //When each single part finishes we cannot notify the write handle that we are done.
    _writeHandle.done();
  }

  @Override
  public void onAbandoned()
  {
    //This can happen if the MultiPartMIMEDataSourceIterator this callback was registered with was used as a data source and it was
    //told to abandon and the abandon finished.
    //We don't need to take any action here.
  }

  @Override
  public void onStreamError(final Throwable throwable)
  {
    //If there was an error obtaining data sources then we notify the writeHandle.

    //Note that there may or may not be a current MultiPartMIMEDataSource. If there was then it may already have
    //invoked _writeHandle.error().
    //Regardless its safe to do it again in case this did not happen.
    _writeHandle.error(throwable);
  }

  MultiPartMIMEChainReaderCallback(final WriteHandle writeHandle, final byte[] normalEncapsulationBoundary)
  {
    _writeHandle = writeHandle;
    _normalEncapsulationBoundary = normalEncapsulationBoundary;
  }

  MultiPartMIMEDataSource getCurrentDataSource()
  {
    return _currentDataSource;
  }
}