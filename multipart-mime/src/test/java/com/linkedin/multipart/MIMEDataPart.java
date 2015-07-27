package com.linkedin.multipart;

import com.linkedin.data.ByteString;
import java.util.Map;


/**
 * Represents in an-memory multipart mime data source used for testing.
 *
 * @author Karim Vidhani
 */
public final class MIMEDataPart
{
  private final ByteString _partData;
  private final Map<String, String> _headers;

  public MIMEDataPart(final ByteString partData, final Map<String, String> headers)
  {
    if (partData == null)
    {
      _partData = ByteString.empty();
    }
    else
    {
      _partData = partData;
    }
    _headers = headers;
  }

  public ByteString getPartData()
  {
    return _partData;
  }

  public Map<String, String> getPartHeaders()
  {
    return _headers;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o)
    {
      return true;
    }

    if (!(o instanceof MIMEDataPart))
    {
      return false;
    }

    final MIMEDataPart that = (MIMEDataPart) o;

    if(!_headers.equals(that.getPartHeaders()))
    {
      return false;
    }

    if(!_partData.equals(that.getPartData()))
    {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = _partData != null ? _partData.hashCode() : 0;
    result = 31 * result + (_headers != null ? _headers.hashCode() : 0);
    return result;
  }
}