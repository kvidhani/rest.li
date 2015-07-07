package com.linkedin.multipart;

import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.compression.streaming.CompositeWriter;
import com.linkedin.r2.message.streaming.ByteStringWriter;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.WriteHandle;
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
public class SandboxMultiPartMIMEWriter {

  private final CompositeWriter _writer;
  private final EntityStream _entityStream;
  private final List<EntityStream> _allDataSources;
  private final String _rawBoundary;

  public class SandboxMultiPartMIMEWriterBuilder {

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

    public SandboxMultiPartMIMEWriterBuilder(final String preamble, final String epilogue) {
      _preamble = preamble;
      _epilogue = epilogue;
      //Append data source for preamble
      if (!_preamble.equalsIgnoreCase("")) {
        final Writer preambleWriter = new ByteStringWriter(ByteString.copyString(_preamble, Charset.forName("US-ASCII")));
        _allDataSources.add(EntityStreams.newEntityStream(preambleWriter));
      }
    }

    public SandboxMultiPartMIMEWriterBuilder() {
      this("", "");
    }

    public SandboxMultiPartMIMEWriterBuilder appendDataSource(final SandboxMultiPartMIMEDataSource dataSource) {

      //Append the boundary and headers for this part as an EntityStream
      _boundaryHeaderByteArrayOutputStream.reset();
      try {
        _boundaryHeaderByteArrayOutputStream.write(_normalEncapsulationBoundary);
        _boundaryHeaderByteArrayOutputStream.write(MultiPartMIMEUtils.CRLF_BYTES);

        if (!dataSource.dataSourceHeaders().isEmpty()) {
          //Serialize the headers
          _boundaryHeaderByteArrayOutputStream
              .write(MultiPartMIMEUtils.serializedHeaders(dataSource.dataSourceHeaders()).copyBytes());
        }

        //Regardless of whether or not there were headers the RFC calls for another CRLF here.
        //If there were no headers we end up with two CRLFs after the boundary
        //If there were headers CRLF_BYTES we end up with one CRLF after the boundary and one after the last header
        _boundaryHeaderByteArrayOutputStream.write(MultiPartMIMEUtils.CRLF_BYTES);


      } catch (IOException ioException) {
        //Should never happen
        throw new IllegalStateException("Serious error when constructing local byte buffer for boundary and header serialization");
      }
      final Writer boundaryHeaderWriter = new ByteStringWriter(ByteString.copy(_boundaryHeaderByteArrayOutputStream.toByteArray()));
      _allDataSources.add(EntityStreams.newEntityStream(boundaryHeaderWriter));
      _allDataSources.add(EntityStreams.newEntityStream(dataSource));
      return this;
    }

    public SandboxMultiPartMIMEWriterBuilder appendSinglePartDataSource(final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {
      SandboxMultiPartMIMEDataSource singlePartMIMEReaderDataSource =
          new MultiPartMIMEReader.SinglePartMIMEReaderDataSource(singlePartMIMEReader);
      _allDataSources.add(EntityStreams.newEntityStream(singlePartMIMEReaderDataSource));
      return this;
    }

    public SandboxMultiPartMIMEWriterBuilder appendMultiPartDataSource(final MultiPartMIMEReader multiPartMIMEReader) {
      final Writer multiPartMIMEReaderWriter = new MultiPartMIMEReaderWriter(multiPartMIMEReader, _normalEncapsulationBoundary);
      _allDataSources.add(EntityStreams.newEntityStream(multiPartMIMEReaderWriter));
      return this;
    }

    public SandboxMultiPartMIMEWriterBuilder appendDataSources(final List<SandboxMultiPartMIMEDataSource> dataSources) {
      for (final SandboxMultiPartMIMEDataSource dataSource : dataSources) {
        appendDataSource(dataSource);
      }
      return this;
    }

    public SandboxMultiPartMIMEWriterBuilder appendSinglePartDataSources(final List<MultiPartMIMEReader.SinglePartMIMEReader> singlePartMIMEReaders) {
      for (MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader : singlePartMIMEReaders) {
        appendSinglePartDataSource(singlePartMIMEReader);
      }
      return this;
    }

    public SandboxMultiPartMIMEWriterBuilder appendMultiPartDataSources(final List<MultiPartMIMEReader> multiPartMIMEReaders) {
      for (MultiPartMIMEReader multiPartMIMEReader : multiPartMIMEReaders) {
        appendMultiPartDataSource(multiPartMIMEReader);
      }
      return this;
    }

    public SandboxMultiPartMIMEWriter build() {
      //Append epilogue
      if (!_epilogue.equalsIgnoreCase("")) {
        final Writer epilogueWriter = new ByteStringWriter(ByteString.copyString(_epilogue, Charset.forName("US-ASCII")));
        _allDataSources.add(EntityStreams.newEntityStream(epilogueWriter));
      }
      return new SandboxMultiPartMIMEWriter(_allDataSources, _rawBoundary);
    }
  }

  private static class MultiPartMIMEReaderWriter implements Writer {

    private final MultiPartMIMEReader _multiPartMIMEReader;
    private final byte[] _normalEncapsulationBoundary;
    private WriteHandle _writeHandle;
    private MultiPartMIMEChainReaderCallback _multiPartMIMEChainReaderCallback = null;

    private MultiPartMIMEReaderWriter(final MultiPartMIMEReader multiPartMIMEReader,
         final byte[] normalEncapsulationBoundary) {
      _multiPartMIMEReader = multiPartMIMEReader;
      _normalEncapsulationBoundary = normalEncapsulationBoundary;
    }

    @Override
    public void onInit(WriteHandle wh) {
      _writeHandle = wh;

    }

    @Override
    public void onWritePossible() {

      if (_multiPartMIMEChainReaderCallback == null) {
        _multiPartMIMEChainReaderCallback = new MultiPartMIMEChainReaderCallback(_writeHandle, _multiPartMIMEReader, _normalEncapsulationBoundary);
        //Since this is not a MultiPartMIMEDataSource we can't use the regular mechanism for reading data.
        //Instead of create a new callback that will use write to the writeHandle using the SinglePartMIMEReader

        _multiPartMIMEReader.registerReaderCallback(_multiPartMIMEChainReaderCallback);

      //Note that by registering here, this will eventually lead to onNewPart() which will then requestPartData()
      //which will eventually lead to onPartDataAvailable() which will then write to the writeHandle thereby
      //honoring the original request here to write data. This initial write here will write out the boundary that this
      //writer is using followed by the headers. This is similar to _transitionToNewDataSource except within
      //a MultiPartMIMEReader.
      } else {
        _multiPartMIMEChainReaderCallback.getCurrentSinglePartReaderDataSource().onWritePossible();
      }

    }

    //todo ang is fixing this to make sure this is invoked by CompositeWriter
    @Override
    public void onAbort(Throwable e) {
      //This will be invoked if R2 tells the composite writer to abort which will then tell this Writer to abort.
      //In this case there is no way to notify the application developer that the MultiPartMIMEReader
      //they provided as a data source has seen a problem.
      //Therefore we will treat this scenario as if an exception occurred while reading (which did happen).

      //We need to have behavior similar to handleExceptions() so that everything is cancelled
      //If there were potentially multiple chains across different servers, then all the readers
      //in the chain need to be shut down.
      //This is in contrast to the case where if one SinglePartReader was sent down as a data source. In that
      //case we notify the custom client MultiPartMIMEReaderCallback and they can recover.

      _multiPartMIMEReader.getR2MultiPartMIMEReader().handleExceptions(e);

      //TODO - open a jira to provide this behavior
    }
  }

  private SandboxMultiPartMIMEWriter(final List<EntityStream> allDataSources, final String rawBoundary) {

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
