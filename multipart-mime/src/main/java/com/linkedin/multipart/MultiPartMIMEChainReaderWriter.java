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


import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;


/**
 * The writer to consume the {@link com.linkedin.multipart.MultiPartMIMEReader} when
 * chaining the entire reader itself as a data source.
 *
 * @author Karim Vidhani
 */
class MultiPartMIMEChainReaderWriter implements Writer
{
  private final MultiPartMIMEReader _multiPartMIMEReader;
  private final byte[] _normalEncapsulationBoundary;
  private WriteHandle _writeHandle;
  private MultiPartMIMEChainReaderCallback _multiPartMIMEChainReaderCallback = null;

  MultiPartMIMEChainReaderWriter(final MultiPartMIMEReader multiPartMIMEReader,
      final byte[] normalEncapsulationBoundary)
  {
    _multiPartMIMEReader = multiPartMIMEReader;
    _normalEncapsulationBoundary = normalEncapsulationBoundary;
  }

  @Override
  public void onInit(WriteHandle wh)
  {
    _writeHandle = wh;
  }

  @Override
  public void onWritePossible()
  {
    if (_multiPartMIMEChainReaderCallback == null)
    {
      _multiPartMIMEChainReaderCallback =
          new MultiPartMIMEChainReaderCallback(_writeHandle, _normalEncapsulationBoundary);
      //Since this is not a MultiPartMIMEDataSource we can't use the regular mechanism for reading data.
      //Instead of create a new callback that will use to write to the writeHandle using the SinglePartMIMEReader.

      _multiPartMIMEReader.registerReaderCallback(_multiPartMIMEChainReaderCallback);
      //Note that by registering here, this will eventually lead to onNewPart() which will then requestPartData()
      //which will eventually lead to onPartDataAvailable() which will then write to the writeHandle thereby
      //honoring the original request here to write data. This initial write here will write out the boundary that this
      //writer is using followed by the headers.

    } else
    {
      //R2 asked us to read after initial setup is done.
      _multiPartMIMEChainReaderCallback.getCurrentSinglePartReader().requestPartData();
    }
  }

  //todo Ang is fixing this to make sure this is invoked by CompositeWriter
  @Override
  public void onAbort(Throwable e)
  {
    //This will be invoked if R2 tells the composite writer to abort which will then tell this Writer to abort.
    //In this case there is no way to notify the application developer that the MultiPartMIMEReader
    //they provided as a data source has seen a problem.
    //Therefore we will treat this scenario as if an exception occurred while reading.

    //We need to have behavior similar to handleExceptions() so that everything is cancelled
    //If there were potentially multiple chains across different servers, then all the readers
    //in the chain need to be shut down.
    //This is in contrast to the case where if one SinglePartReader was sent down as a data source. In that
    //case we notify the custom client MultiPartMIMEReaderCallback and they can recover.
    _multiPartMIMEReader.getR2MultiPartMIMEReader().handleExceptions(e);
  }
}