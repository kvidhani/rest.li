package com.linkedin.multipart;

import javax.mail.internet.ContentType;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.ParameterList;
import junit.framework.Assert;


/**
 * Created by kvidhani on 7/14/15.
 */
//todo maybe a better way to expose this?
public final class DataSources {

   static final String HEADER_CONTENT_TYPE = "Content-Type";
   static final String _textPlainType = "text/plain";
   static final String _binaryType = "application/octet-stream";

  static MimeBodyPart _smallDataSource; //Represents a small part with headers and a body composed of simple text
  static MimeBodyPart _largeDataSource; //Represents a large part with headers and a body composed of simple text
  static MimeBodyPart _headerLessBody; //Represents a part with a body and no headers
  static MimeBodyPart _bodyLessBody; //Represents a part with headers but no body
  static MimeBodyPart _bytesBody; //Represents a part with bytes
  static MimeBodyPart _purelyEmptyBody; //Represents a part with no headers and no body

  //Disable instantiation
  private DataSources() {}

  static {
    try {
      //Small body.
      {
        final String body = "A tiny body";
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
