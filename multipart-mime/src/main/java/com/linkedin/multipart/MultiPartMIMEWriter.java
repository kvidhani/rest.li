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
import com.linkedin.r2.filter.compression.streaming.CompositeWriter;
import com.linkedin.r2.message.streaming.ByteStringWriter;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.Writer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


/**
 * Used to aggregate multiple different data sources and subsequently construct a multipart mime
 * envelope.
 *
 * @author Karim Vidhani
 */
public final class MultiPartMIMEWriter
{
  private final CompositeWriter _writer;
  private final EntityStream _entityStream;
  private final List<EntityStream> _allDataSources;
  private final String _rawBoundary;

  /**
   * Builder to create the MultiPartMIMEWriter.
   */
  public static class Builder
  {
    private List<EntityStream> _allDataSources = new ArrayList<EntityStream>();
    private final String _preamble;
    private final String _epilogue;
    private final ByteArrayOutputStream _boundaryHeaderByteArrayOutputStream = new ByteArrayOutputStream();

    //Generate the boundary
    private final String _rawBoundary = MultiPartMIMEUtils.generateBoundary();
    //As per the RFC there must two preceding hyphen characters on each boundary between each parts
    private final byte[] _normalEncapsulationBoundary =
        (MultiPartMIMEUtils.CRLF_STRING + "--" + _rawBoundary).getBytes(Charset.forName("US-ASCII"));
    //As per the RFC the final boundary has two extra hyphens at the end
    private final byte[] _finalEncapsulationBoundary =
        (MultiPartMIMEUtils.CRLF_STRING + "--" + _rawBoundary + "--").getBytes(Charset.forName("US-ASCII"));

    /**
     * Create a MultiPartMIMEWriter using the specified preamble and epilogue.
     *
     * @param preamble to be placed before the multipart mime envelope according to the RFC.
     * @param epilogue to be placed after the multipart mime enveloped according to the RFC
     */
    public Builder(final String preamble, final String epilogue)
    {
      _preamble = preamble;
      _epilogue = epilogue;
      //Append data source for preamble
      if (!_preamble.equalsIgnoreCase(""))
      {
        final Writer preambleWriter =
            new ByteStringWriter(ByteString.copyString(_preamble, Charset.forName("US-ASCII")));
        _allDataSources.add(EntityStreams.newEntityStream(preambleWriter));
      }
    }

    /**
     * Create a MultiPartMIMEWriter without a preamble or epilogue.
     */
    public Builder()
    {
      this("", "");
    }

    /**
     * Append a {@link com.linkedin.multipart.MultiPartMIMEDataSource} to be placed in the multipart mime envelope.
     *
     * @param dataSource the data source to be added.
     */
    public Builder appendDataSource(final MultiPartMIMEDataSource dataSource)
    {
      ByteString serializedBoundaryAndHeaders = null;
      try
      {
        serializedBoundaryAndHeaders =
            MultiPartMIMEUtils.serializeBoundaryAndHeaders(_normalEncapsulationBoundary, dataSource);
      }
      catch (IOException ioException)
      {
        //Should never happen
        throw new IllegalStateException(
            "Serious error when constructing local byte buffer for the boundary and headers!");
      }

      //Note that that nothing happens if there is an abort in the middle of writing a boundary or headers.
      final Writer boundaryHeaderWriter = new ByteStringWriter(serializedBoundaryAndHeaders);
      _allDataSources.add(EntityStreams.newEntityStream(boundaryHeaderWriter));
      _allDataSources.add(EntityStreams.newEntityStream(dataSource));
      return this;
    }

    /**
     * Append a {@link com.linkedin.multipart.MultiPartMIMEReader} to be used as a data source
     * within the multipart mime envelope. All the individual parts from the {@link com.linkedin.multipart.MultiPartMIMEReader}
     * will be placed one by one into this new envelope with boundaries replaced.
     *
     * @param multiPartMIMEReader
     */
    public Builder appendMultiPartDataSource(final MultiPartMIMEReader multiPartMIMEReader)
    {
      final Writer multiPartMIMEReaderWriter =
          new MultiPartMIMEChainReaderWriter(multiPartMIMEReader, _normalEncapsulationBoundary);
      _allDataSources.add(EntityStreams.newEntityStream(multiPartMIMEReaderWriter));
      return this;
    }

    /**
     * Append multiple {@link com.linkedin.multipart.MultiPartMIMEDataSource}s into the multipart mime envelope.
     *
     * @param dataSources the data sources to be added.
     */
    public Builder appendDataSources(final List<MultiPartMIMEDataSource> dataSources)
    {
      for (final MultiPartMIMEDataSource dataSource : dataSources)
      {
        appendDataSource(dataSource);
      }
      return this;
    }

    /**
     * Append multiple {@link com.linkedin.multipart.MultiPartMIMEReader}s into the multipart mime envelope.
     *
     * @param multiPartMIMEReaders
     */
    public Builder appendMultiPartDataSources(final List<MultiPartMIMEReader> multiPartMIMEReaders)
    {
      for (MultiPartMIMEReader multiPartMIMEReader : multiPartMIMEReaders)
      {
        appendMultiPartDataSource(multiPartMIMEReader);
      }
      return this;
    }

    /**
     * Construct and return the newly formed MultiPartMIMEWriter.
     */
    public MultiPartMIMEWriter build()
    {
      //Append the final boundary
      _boundaryHeaderByteArrayOutputStream.reset();
      try
      {
        _boundaryHeaderByteArrayOutputStream.write(_finalEncapsulationBoundary);
      }
      catch (IOException ioException)
      {
        //Should never happen
        throw new IllegalStateException("Serious error when constructing local byte buffer for the final boundary!");
      }
      final Writer finalBoundaryWriter =
          new ByteStringWriter(ByteString.copy(_boundaryHeaderByteArrayOutputStream.toByteArray()));
      _allDataSources.add(EntityStreams.newEntityStream(finalBoundaryWriter));

      //Append epilogue
      if (!_epilogue.equalsIgnoreCase(""))
      {
        final Writer epilogueWriter =
            new ByteStringWriter(ByteString.copyString(_epilogue, Charset.forName("US-ASCII")));
        _allDataSources.add(EntityStreams.newEntityStream(epilogueWriter));
      }

      return new MultiPartMIMEWriter(_allDataSources, _rawBoundary);
    }
  }

  private MultiPartMIMEWriter(final List<EntityStream> allDataSources, final String rawBoundary)
  {
    _allDataSources = allDataSources;
    _rawBoundary = rawBoundary;
    _writer = new CompositeWriter(_allDataSources);
    _entityStream = EntityStreams.newEntityStream(_writer);
  }

  public EntityStream getEntityStream()
  {
    return _entityStream;
  }

  String getBoundary()
  {
    return _rawBoundary;
  }
}