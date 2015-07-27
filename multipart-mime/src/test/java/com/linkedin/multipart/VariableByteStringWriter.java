package com.linkedin.multipart;

import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.streaming.ByteStringWriter;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import com.linkedin.util.ArgumentUtil;
import org.testng.Assert;

import java.util.Random;

/**
 * Writes bytes out from a {@link com.linkedin.data.ByteString} based on customizable
 * chunk sizes.
 *
 * @author Karim Vidhani
 */
public class VariableByteStringWriter implements Writer {
    private final ByteString _content;
    private final int _chunkSize;
    private int _offset;
    private WriteHandle _wh;

    public VariableByteStringWriter(final ByteString content, final int chunkSize)
    {
        ArgumentUtil.notNull(content, "content");
        _content = content;
        _chunkSize = chunkSize;
        _offset = 0;
    }

    @Override
    public void onInit(WriteHandle wh)
    {
        _wh = wh;
    }

    @Override
    public void onWritePossible()
    {
        while(_wh.remaining() > 0)
        {
            if (_offset == _content.length())
            {
                _wh.done();
                break;
            }
            int bytesToWrite = Math.min(_chunkSize, _content.length() - _offset);
            _wh.write(_content.slice(_offset, bytesToWrite));
            _offset += bytesToWrite;
        }
    }

    @Override
    public void onAbort(Throwable ex)
    {
        Assert.fail();
    }
}