package com.linkedin.multipart;

import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import com.linkedin.util.ArgumentUtil;

import java.util.Random;

/**
 * Created by kvidhani on 7/4/15.
 */
public class VariableByteStringWriter implements Writer {
    private final ByteString _content;
    private int _offset;
    private WriteHandle _wh;
    private static final Random _random = new Random();
    private final NumberBytesToWrite _numberBytesToWrite;

    public VariableByteStringWriter(ByteString content) {
        this(content, NumberBytesToWrite.ONE);
    }

    public VariableByteStringWriter(ByteString content, NumberBytesToWrite numberBytesToWrite) {
        ArgumentUtil.notNull(content, "content");
        _content = content;
        _offset = 0;
        _numberBytesToWrite = numberBytesToWrite;
    }

    @Override
    public void onInit(WriteHandle wh) {
        _wh = wh;
    }

    @Override
    public void onWritePossible() {
        while (_wh.remaining() > 0) {
            if (_offset == _content.length()) {
                _wh.done();
                break;
            }
            final int bytesToWrite;
            if (_numberBytesToWrite == NumberBytesToWrite.ONE) {
                bytesToWrite = 1;
            } else {
                //Some random amount of bytes based on how much is left
                bytesToWrite = _random.nextInt(_content.length() - _offset);
            }

            _wh.write(_content.slice(_offset, bytesToWrite));
            _offset += bytesToWrite;
        }
    }

    @Override
    public void onAbort(Throwable ex) {
        // do nothing
    }

    public enum NumberBytesToWrite {
        ONE,
        RANDOM
    }
}
