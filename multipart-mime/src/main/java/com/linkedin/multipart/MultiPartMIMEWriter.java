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
 * Created by kvidhani on 7/6/15.
 */
//TODO note that Ang will look into fixing composite writer
//todo evaluate all sitautions where callbacks can throw....ALL CALLBACKS CAN THROW POTENTIALLY!

public final class MultiPartMIMEWriter {

  private final CompositeWriter _writer;
  private final EntityStream _entityStream;
  private final List<EntityStream> _allDataSources;
  private final String _rawBoundary;

  public static class MultiPartMIMEWriterBuilder {

    private List<EntityStream> _allDataSources = new ArrayList<EntityStream>();
    private final String _preamble;
    private final String _epilogue;
    private final ByteArrayOutputStream _boundaryHeaderByteArrayOutputStream = new ByteArrayOutputStream();

    //Generate the boundary
    private final String _rawBoundary = MultiPartMIMEUtils.generateBoundary();
    //As per the RFC there must two preceding hyphen characters on each boundary between each parts
    private final byte[] _normalEncapsulationBoundary = (MultiPartMIMEUtils.CRLF_STRING + "--" + _rawBoundary).getBytes(Charset.forName("US-ASCII"));
    //As per the RFC the final boundary has two extra hyphens at the end
    private final byte[] _finalEncapsulationBoundary = (MultiPartMIMEUtils.CRLF_STRING + "--" + _rawBoundary + "--").getBytes(Charset.forName("US-ASCII"));

    public MultiPartMIMEWriterBuilder(final String preamble, final String epilogue) {
      _preamble = preamble;
      _epilogue = epilogue;
      //Append data source for preamble
      if (!_preamble.equalsIgnoreCase("")) {
        final Writer preambleWriter = new ByteStringWriter(ByteString.copyString(_preamble, Charset.forName("US-ASCII")));
        _allDataSources.add(EntityStreams.newEntityStream(preambleWriter));
      }
    }

    public MultiPartMIMEWriterBuilder() {
      this("", "");
    }

    public MultiPartMIMEWriterBuilder appendDataSource(final MultiPartMIMEDataSource dataSource) {

      ByteString serializedBoundaryAndHeaders = null;
      try {
        serializedBoundaryAndHeaders = MultiPartMIMEUtils.serializeBoundaryAndHeaders(_normalEncapsulationBoundary, dataSource);
      } catch (IOException ioException) {
        //Should never happen
        throw new IllegalStateException("Serious error when constructing local byte buffer for the boundary and headers!");
      }

      //Note that that nothing happens if there is an abort in the middle of writing a boundary or headers.
      final Writer boundaryHeaderWriter = new ByteStringWriter(serializedBoundaryAndHeaders);
      _allDataSources.add(EntityStreams.newEntityStream(boundaryHeaderWriter));
      _allDataSources.add(EntityStreams.newEntityStream(dataSource));
      return this;
    }

    public MultiPartMIMEWriterBuilder appendMultiPartDataSource(final MultiPartMIMEReader multiPartMIMEReader) {
      final Writer multiPartMIMEReaderWriter = new MultiPartMIMEChainReaderWriter(multiPartMIMEReader, _normalEncapsulationBoundary);
      _allDataSources.add(EntityStreams.newEntityStream(multiPartMIMEReaderWriter));
      return this;
    }

    public MultiPartMIMEWriterBuilder appendDataSources(final List<MultiPartMIMEDataSource> dataSources) {
      for (final MultiPartMIMEDataSource dataSource : dataSources) {
        appendDataSource(dataSource);
      }
      return this;
    }

    public MultiPartMIMEWriterBuilder appendMultiPartDataSources(final List<MultiPartMIMEReader> multiPartMIMEReaders) {
      for (MultiPartMIMEReader multiPartMIMEReader : multiPartMIMEReaders) {
        appendMultiPartDataSource(multiPartMIMEReader);
      }
      return this;
    }

    public MultiPartMIMEWriter build() {

      //Append the final boundary
      _boundaryHeaderByteArrayOutputStream.reset();
      try {
        _boundaryHeaderByteArrayOutputStream.write(_finalEncapsulationBoundary);

      } catch (IOException ioException) {
        //Should never happen
        throw new IllegalStateException("Serious error when constructing local byte buffer for the final boundary!");
      }
      final Writer finalBoundaryWriter = new ByteStringWriter(ByteString.copy(_boundaryHeaderByteArrayOutputStream.toByteArray()));
      _allDataSources.add(EntityStreams.newEntityStream(finalBoundaryWriter));

      //Append epilogue
      if (!_epilogue.equalsIgnoreCase("")) {
        final Writer epilogueWriter = new ByteStringWriter(ByteString.copyString(_epilogue, Charset.forName("US-ASCII")));
        _allDataSources.add(EntityStreams.newEntityStream(epilogueWriter));
      }

      return new MultiPartMIMEWriter(_allDataSources, _rawBoundary);
    }
  }

  private MultiPartMIMEWriter(final List<EntityStream> allDataSources, final String rawBoundary) {

    _allDataSources = allDataSources;
    _rawBoundary = rawBoundary;
    _writer = new CompositeWriter(_allDataSources);
    _entityStream = EntityStreams.newEntityStream(_writer);
  }

  public EntityStream getEntityStream() {
    return _entityStream;
  }

  String getBoundary() {
    return _rawBoundary;
  }

}
