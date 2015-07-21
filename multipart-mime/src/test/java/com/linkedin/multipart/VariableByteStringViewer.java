package com.linkedin.multipart;

import com.linkedin.data.ByteString;
import com.linkedin.util.ArgumentUtil;

/**
 * Created by kvidhani on 7/20/15.
 */

//Unit test equivalent of VariableByteStringWriter
class VariableByteStringViewer {
    private final ByteString _content;
    private final int _chunkSize;
    private int _offset;

    public VariableByteStringViewer(final ByteString content, final int chunkSize)
    {
        ArgumentUtil.notNull(content, "content");
        _content = content;
        _chunkSize = chunkSize;
        _offset = 0;
    }

    public ByteString onWritePossible()
    {
        if (_offset == _content.length())
        {
            return ByteString.empty();
        }
        int bytesToWrite = Math.min(_chunkSize, _content.length() - _offset);
        ByteString slice = _content.slice(_offset, bytesToWrite);
        _offset += bytesToWrite;
        return slice;

    }

}

