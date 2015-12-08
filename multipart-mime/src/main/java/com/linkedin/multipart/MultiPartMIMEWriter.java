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
import com.linkedin.r2.message.stream.entitystream.ByteStringWriter;
import com.linkedin.r2.message.stream.entitystream.CompositeWriter;
import com.linkedin.r2.message.stream.entitystream.EntityStream;
import com.linkedin.r2.message.stream.entitystream.EntityStreams;
import com.linkedin.r2.message.stream.entitystream.Writer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


/**
 * Used to aggregate multiple different data sources and subsequently construct a multipart mime envelope.
 *
 * @author Karim Vidhani
 */
public final class MultiPartMIMEWriter
{
  private final CompositeWriter _writer;
  private final EntityStream _entityStream;
  private final List<Writer> _allDataSources;
  private final String _rawBoundary;

  /**
   * Builder to create the MultiPartMIMEWriter.
   */
  public static class Builder
  {
    private List<Writer> _allDataSources = new ArrayList<Writer>();
    private final String _preamble;
    private final String _epilogue;

    //Generate the boundary
    private final String _rawBoundary = MultiPartMIMEUtils.generateBoundary();
    //As per the RFC there must two preceding hyphen characters on each boundary between each parts
    private final byte[] _normalEncapsulationBoundary =
        (MultiPartMIMEUtils.CRLF_STRING + "--" + _rawBoundary).getBytes(Charset.forName("US-ASCII"));
    //As per the RFC the final boundary has two extra hyphens at the end
    private final byte[] _finalEncapsulationBoundary =
        (MultiPartMIMEUtils.CRLF_STRING + "--" + _rawBoundary + "--").getBytes(Charset.forName("US-ASCII"));

    /**
     * Create a MultiPartMIMEWriter Builder using the specified preamble and epilogue.
     *
     * @param preamble to be placed before the multipart mime envelope according to the RFC.
     * @param epilogue to be placed after the multipart mime enveloped according to the RFC.
     * @return the builder to continue building.
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
        _allDataSources.add(preambleWriter);
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
     * @return the builder to continue building.
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
        throw new IllegalStateException("Serious error when constructing local byte buffer for the boundary and headers!");
      }

      final Writer boundaryHeaderWriter = new ByteStringWriter(serializedBoundaryAndHeaders);
      _allDataSources.add(boundaryHeaderWriter);
      _allDataSources.add(dataSource);
      return this;
    }

    /**
     * Append a {@link MultiPartMIMEDataSourceIterator} to be used as a non-nested data source
     * within the multipart mime envelope.
     *
     * All the individual parts read using the {@link MultiPartMIMEDataSourceIterator}
     * will be placed one by one into this new envelope with boundaries replaced.
     *
     * @param multiPartMIMEDataSourceIterator the {@link MultiPartMIMEDataSourceIterator} that will be used
     *                                  to produce multiple parts to append.
     * @return the builder to continue building.
     */
    public Builder appendDataSourceIterator(final MultiPartMIMEDataSourceIterator multiPartMIMEDataSourceIterator)
    {
      final Writer multiPartMIMEReaderWriter =
          new MultiPartMIMEChainReaderWriter(multiPartMIMEDataSourceIterator, _normalEncapsulationBoundary);
      _allDataSources.add(multiPartMIMEReaderWriter);
      return this;
    }

    /**
     * Append multiple {@link com.linkedin.multipart.MultiPartMIMEDataSource}s into the multipart mime envelope.
     *
     * @param dataSources the data sources to be added.
     * @return the builder to continue building.
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
     * Construct and return the newly formed {@link com.linkedin.multipart.MultiPartMIMEWriter}.
     * @return the fully constructed {@link com.linkedin.multipart.MultiPartMIMEWriter}.
     */
    public MultiPartMIMEWriter build()
    {
      //Append the final boundary
      final ByteArrayOutputStream finalBoundaryByteArrayOutputStream = new ByteArrayOutputStream();
      try
      {
        finalBoundaryByteArrayOutputStream.write(_finalEncapsulationBoundary);
      }
      catch (IOException ioException)
      {
        //Should never happen
        throw new IllegalStateException("Serious error when constructing local byte buffer for the final boundary!");
      }
      final Writer finalBoundaryWriter =
          new ByteStringWriter(ByteString.copy(finalBoundaryByteArrayOutputStream.toByteArray()));
      _allDataSources.add(finalBoundaryWriter);

      //Append epilogue
      if (!_epilogue.equalsIgnoreCase(""))
      {
        final Writer epilogueWriter =
            new ByteStringWriter(ByteString.copyString(_epilogue, Charset.forName("US-ASCII")));
        _allDataSources.add(epilogueWriter);
      }

      return new MultiPartMIMEWriter(_allDataSources, _rawBoundary);
    }
  }

  private MultiPartMIMEWriter(final List<Writer> allDataSources, final String rawBoundary)
  {
    _allDataSources = allDataSources;
    _rawBoundary = rawBoundary;
    _writer = new CompositeWriter(_allDataSources);
    _entityStream = EntityStreams.newEntityStream(_writer);
  }

  /**
   * Aborts all data sources contained with this {@link com.linkedin.multipart.MultiPartMIMEWriter}. This is useful
   * to invoke when many data sources have been collected and this {@link com.linkedin.multipart.MultiPartMIMEWriter} has
   * been created, but an exception (or any other event) is observed and a {@link com.linkedin.r2.message.stream.StreamRequest}
   * or a {@link com.linkedin.r2.message.stream.StreamResponse} will no longer be sent. In such a case it is prudent to
   * clean up all data sources.
   *
   * The abandon behavior can be different for each data source passed in.
   *
   * 1. If the data source passed in is a custom {@link com.linkedin.multipart.MultiPartMIMEDataSource}, then it will be
   * invoked on {@link com.linkedin.r2.message.stream.entitystream.Writer#onAbort(java.lang.Throwable)}. At this point
   * the custom data source can perform any cleanup necessary. Note that the custom {@link com.linkedin.multipart.MultiPartMIMEDataSource}
   * will be able to see the Throwable that is passed into this method.
   *
   * 2. If the data source passed in is a {@link MultiPartMIMEDataSourceIterator}, then all data sources
   * represented by this MultiPartMIMEPartIterator will be read and abandoned. See {@link MultiPartMIMEDataSourceIterator#abortAllDataSources()}.
   * In this case the Throwable that is passed into this method will not be used.
   *
   * @param throwable the Throwable that caused the abandonment to happen.
   */
  public void abortAllDataSources(final Throwable throwable)
  {
    //Note that we can't simply do _writer.onAbort(throwable) since reading from this CompositeWriter may not have begun yet.
    for (Writer writer : _allDataSources)
    {
      writer.onAbort(throwable);
    }
  }

  /**
   * This should never be used by external consumers.
   *
   * Returns the underlying {@link com.linkedin.r2.message.stream.entitystream.EntityStream} that will be used
   * for the {@link com.linkedin.r2.message.stream.StreamRequest} or {@link com.linkedin.r2.message.stream.StreamResponse}.
   *
   * @return the {@link com.linkedin.r2.message.stream.entitystream.EntityStream} representing a Writer responsible
   *         for writing the payload of a {@link com.linkedin.r2.message.stream.StreamRequest} or
   *         {@link com.linkedin.r2.message.stream.StreamResponse}.
   */
  public EntityStream getEntityStream()
  {
    return _entityStream;
  }

  String getBoundary()
  {
    return _rawBoundary;
  }
}