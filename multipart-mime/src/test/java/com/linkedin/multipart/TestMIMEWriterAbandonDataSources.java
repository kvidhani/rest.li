package com.linkedin.multipart;


import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;


/**
 * This test verifies that upon invocation of {@link MultiPartMIMEWriter#abandonDataSources(java.lang.Throwable)},
 * that each type of data source is abandoned appropriately.
 *
 * @author Karim Vidhani
 */
public class TestMIMEWriterAbandonDataSources extends AbstractMIMEUnitTest
{
  private MultiPartMIMEReader _reader;

  @Test
  public void abandonAllTypesDataSource() throws Exception
  {
    //Create a MultiPartMIMEReader, SinglePartMIMEReader and custom data source all as data sources for a writer.
    final MultiPartMIMEReader multiPartMIMEReader = mock(MultiPartMIMEReader.class);
    final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader = mock(MultiPartMIMEReader.SinglePartMIMEReader.class);
    final MultiPartMIMEDataSource multiPartMIMEDataSource = mock(MultiPartMIMEDataSource.class);

    Mockito.doCallRealMethod().when(singlePartMIMEReader).onAbort(any(Throwable.class));

    final MultiPartMIMEWriter writer = new MultiPartMIMEWriter.Builder("abc", "123")
        .appendDataSource(singlePartMIMEReader)
        .appendDataSourcePartIterator(multiPartMIMEReader)
        .appendDataSource(multiPartMIMEDataSource)
        .build();

    final Throwable throwable = new NullPointerException("Some exception");
    writer.abandonDataSources(throwable);

    //The MultiPartMIMEReader should have been abandoned.
    verify(multiPartMIMEReader, times(1)).abandonAllParts();

    //The SinglePartMIMEReader should also have its part abandoned.
    verify(singlePartMIMEReader, times(1)).abandonPart();
    verify(singlePartMIMEReader, times(1)).onAbort(throwable);
    verify(singlePartMIMEReader, times(1)).dataSourceHeaders();

    //The custom data source should have been told to abort.
    verify(multiPartMIMEDataSource, times(1)).onAbort(throwable);
    verify(multiPartMIMEDataSource, times(1)).dataSourceHeaders();

    verifyNoMoreInteractions(multiPartMIMEReader);
    verifyNoMoreInteractions(singlePartMIMEReader);
    verifyNoMoreInteractions(multiPartMIMEDataSource);
  }
}
