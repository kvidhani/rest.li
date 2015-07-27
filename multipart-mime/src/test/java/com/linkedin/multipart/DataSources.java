package com.linkedin.multipart;

import javax.mail.internet.ContentType;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.ParameterList;

import com.google.common.collect.ImmutableMap;
import com.linkedin.data.ByteString;
import junit.framework.Assert;

import java.util.Collections;
import java.util.Map;


/**
 * @author Karim Vidhani
 *
 * Shared Javax mail multipart mime data sources for tests.
 */
public final class DataSources {

    static final String HEADER_CONTENT_TYPE = "Content-Type";
    static final String _textPlainType = "text/plain";
    static final String _binaryType = "application/octet-stream";

    //Javax mail data sources
    static MimeBodyPart _tinyDataSource; //Represents a tiny part with no headers. Used exclusively for the stack overflow test.
    static MimeBodyPart _smallDataSource; //Represents a small part with headers and a body composed of simple text
    static MimeBodyPart _largeDataSource; //Represents a large part with headers and a body composed of simple text
    static MimeBodyPart _headerLessBody; //Represents a part with a body and no headers
    static MimeBodyPart _bodyLessBody; //Represents a part with headers but no body
    static MimeBodyPart _bytesBody; //Represents a part with bytes
    static MimeBodyPart _purelyEmptyBody; //Represents a part with no headers and no body

    //Non javax, custom data sources
    static MIMEDataPart _bodyA;
    static MIMEDataPart _bodyB;
    static MIMEDataPart _bodyC;
    static MIMEDataPart _bodyD;
    static MIMEDataPart _body1;
    static MIMEDataPart _body2;
    static MIMEDataPart _body3;
    static MIMEDataPart _body4;
    static MIMEDataPart _body5;

    //Disable instantiation
    private DataSources() {
    }

    static {
        //Non javax mail sources:
        final byte[] bodyAbytes = "bodyA".getBytes();
        final Map<String, String> bodyAHeaders = ImmutableMap.of("headerA", "valueA");
        _bodyA = new MIMEDataPart(ByteString.copy(bodyAbytes), bodyAHeaders);

        final byte[] bodyBbytes = "bodyB".getBytes();
        final Map<String, String> bodyBHeaders = ImmutableMap.of("headerB", "valueB");
        _bodyB = new MIMEDataPart(ByteString.copy(bodyBbytes), bodyBHeaders);

        //body c has no headers
        final byte[] bodyCbytes = "bodyC".getBytes();
        _bodyC = new MIMEDataPart(ByteString.copy(bodyCbytes), Collections.<String, String>emptyMap());

        final byte[] bodyDbytes = "bodyD".getBytes();
        final Map<String, String> bodyDHeaders = ImmutableMap.of("headerD", "valueD");
        _bodyD = new MIMEDataPart(ByteString.copy(bodyDbytes), bodyDHeaders);

        final byte[] body1bytes = "body1".getBytes();
        final Map<String, String> body1Headers = ImmutableMap.of("header1", "value1");
        _body1 = new MIMEDataPart(ByteString.copy(body1bytes), body1Headers);

        final byte[] body2bytes = "body2".getBytes();
        final Map<String, String> body2Headers = ImmutableMap.of("header2", "value2");
        _body2 = new MIMEDataPart(ByteString.copy(body2bytes), body2Headers);

        //body 3 is completely empty
        _body3 = new MIMEDataPart(ByteString.empty(), Collections.<String, String>emptyMap());

        final byte[] body4bytes = "body4".getBytes();
        final Map<String, String> body4Headers = ImmutableMap.of("header4", "value4");
        _body4 = new MIMEDataPart(ByteString.copy(body4bytes), body4Headers);

        final byte[] localInputStreamBytes = "local input stream".getBytes();
        final Map<String, String> localInputStreamHeaders = ImmutableMap.of("local1", "local2");
        _body5 = new MIMEDataPart(ByteString.copy(localInputStreamBytes), localInputStreamHeaders);

        //Now create the javax data sources:
        try {
            //Tiny body.
            {
                final String body = "1";
                final MimeBodyPart dataPart = new MimeBodyPart();
                final ContentType contentType = new ContentType(_textPlainType);
                dataPart.setContent(body, contentType.getBaseType());
                _tinyDataSource = dataPart;
            }

            //Small body.
            {
                final String body = "A small body";
                final MimeBodyPart dataPart = new MimeBodyPart();
                final ContentType contentType = new ContentType(_textPlainType);
                dataPart.setContent(body, contentType.getBaseType());
                dataPart.setHeader(HEADER_CONTENT_TYPE, contentType.toString());
                dataPart.setHeader("SomeCustomHeader", "SomeCustomValue");
                _smallDataSource = dataPart;
            }

            //Large body. Something bigger then the size of the boundary with folded headers.
            {
                final String body = "Has at possim tritani laoreet, vis te meis verear. Vel no vero quando oblique, eu blandit placerat nec, vide facilisi recusabo nec te. Veri labitur sensibus eum id. Quo omnis "
                        + "putant erroribus ad, nonumes copiosae percipit in qui, id cibo meis clita pri. An brute mundi quaerendum duo, eu aliquip facilisis sea, eruditi invidunt dissentiunt eos ea.";
                final MimeBodyPart dataPart = new MimeBodyPart();
                final ContentType contentType = new ContentType(_textPlainType);
                dataPart.setContent(body, contentType.getBaseType());
                //Modify the content type header to use folding. We will also use multiple headers that use folding to verify
                //the integrity of the reader. Note that the Content-Type header uses parameters which are key/value pairs
                //separated by '='. Note that we do not use two consecutive CRLFs anywhere since our implementation
                //does not support this.
                final StringBuffer contentTypeBuffer = new StringBuffer(contentType.toString());
                contentTypeBuffer.append(";\r\n\t\t\t");
                contentTypeBuffer.append("parameter1= value1");
                contentTypeBuffer.append(";\r\n   \t");
                contentTypeBuffer.append("parameter2= value2");

                //This is a custom header which is folded. It does not use parameters so it's values are separated by commas.
                final StringBuffer customHeaderBuffer = new StringBuffer();
                customHeaderBuffer.append("CustomValue1");
                customHeaderBuffer.append(",\r\n\t  \t");
                customHeaderBuffer.append("CustomValue2");
                customHeaderBuffer.append(",\r\n ");
                customHeaderBuffer.append("CustomValue3");

                dataPart.setHeader(HEADER_CONTENT_TYPE, contentTypeBuffer.toString());
                dataPart.setHeader("AnotherCustomHeader", "AnotherCustomValue");
                dataPart.setHeader("FoldedHeader", customHeaderBuffer.toString());
                _largeDataSource = dataPart;
            }

            //Header-less body. This has a body but no headers.
            {
                final String body = "A body without any headers.";
                final MimeBodyPart dataPart = new MimeBodyPart();
                final ContentType contentType = new ContentType(_textPlainType);
                dataPart.setContent(body, contentType.getBaseType());
                _headerLessBody = dataPart;
            }

            //Body-less body. This has no body but does have headers, some of which are folded.
            {
                final MimeBodyPart dataPart = new MimeBodyPart();
                final ParameterList parameterList = new ParameterList();
                parameterList.set("AVeryVeryVeryVeryLongHeader", "AVeryVeryVeryVeryLongValue");
                parameterList.set("AVeryVeryVeryVeryLongHeader2", "AVeryVeryVeryVeryLongValue2");
                parameterList.set("AVeryVeryVeryVeryLongHeader3", "AVeryVeryVeryVeryLongValue3");
                parameterList.set("AVeryVeryVeryVeryLongHeader4", "AVeryVeryVeryVeryLongValue4");
                final ContentType contentType = new ContentType("text", "plain", parameterList);
                dataPart.setContent("", contentType.getBaseType());
                dataPart.setHeader(HEADER_CONTENT_TYPE, contentType.toString());
                dataPart.setHeader("YetAnotherCustomHeader", "YetAnotherCustomValue");
                _bodyLessBody = dataPart;
            }

            //Bytes body. A body that uses a content type different then just text/plain.
            {
                final byte[] body = new byte[20];
                for (int i = 0; i < body.length; i++) {
                    body[i] = (byte) i;
                }
                final MimeBodyPart dataPart = new MimeBodyPart();
                final ContentType contentType = new ContentType(_binaryType);
                dataPart.setContent(body, contentType.getBaseType());
                dataPart.setHeader(HEADER_CONTENT_TYPE, contentType.toString());
                _bytesBody = dataPart;
            }

            //Purely empty body. This has no body or headers.
            {
                final MimeBodyPart dataPart = new MimeBodyPart();
                final ContentType contentType = new ContentType(_textPlainType);
                dataPart.setContent("", contentType.getBaseType()); //Mail requires content so we do a bit of a hack here.
                _purelyEmptyBody = dataPart;
            }
        } catch (Exception exception) {
            Assert.fail();
        }
    }
}