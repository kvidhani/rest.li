package com.linkedin.multipart.writer;

import com.linkedin.r2.message.rest.StreamRequestBuilder;

import java.net.URI;
import java.util.Map;
import java.util.Random;

/**
 * Created by kvidhani on 6/2/15.
 */
public class MultiPartMIMEStreamRequestBuilder {
    private final URI _uri;
    private final String _contentType;
    private final Map<String, String> _headers;
    private final MultiPartMIMEWriter _writer;
    private static final char[] MULTIPART_CHARS =
            "-_1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();


    public MultiPartMIMEStreamRequestBuilder(final URI uri, final String contentType,
                                             final Map<String, String> headers,
                                             final MultiPartMIMEWriter writer) {
        _uri = uri;
        _contentType = contentType;
        _headers = headers;
        _writer = writer;
    }

    //todo - This will create the appropriate multipart mime content-type header
    //add it to the headers and create a StreamRequest

    //todo also do this for StreamResponse

    //we will have to wrap and delegate to StreamRequestBuilder


    private String generateBoundary() {
        final StringBuilder buffer = new StringBuilder();
        final Random rand = new Random();
        final int count = rand.nextInt(11) + 30; // a random size from 30 to 40
        for (int i = 0; i < count; i++) {
            buffer.append(MULTIPART_CHARS[rand.nextInt(MULTIPART_CHARS.length)]);
        }
        //RFC 2046 states a limited character set for the boundary
        //Which means we can encode using ASCII todo
        return buffer.toString();
    }

}
