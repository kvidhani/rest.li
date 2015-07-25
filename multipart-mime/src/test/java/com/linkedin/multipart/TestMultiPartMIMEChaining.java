package com.linkedin.multipart;

/**
 * Created by kvidhani on 7/22/15.
 */
public class TestMultiPartMIMEChaining {


    //todo change the name fo this file

    //todo test the case where when we chain, and register a new top level multipart mime reader callback, we throw
    //from:
    //multipart mime reader callback:
    //onNewPart both in the main logic and also in callback registration


  //1. Echo MM reader. Server gets a MM reader and simply sends it back.
  //A. Echo single.
  //B. Chain a single to another server
  //C. Chain a MM to another server.

  //2. Client sends a MM request. On the server we append a few local input streams and tag on the MM and send that down without
  //even reading. Verify the server gets everything.

  //3. Client sends a MM request. On the server we consume a few parts and send the rest to another server with a
  //few of our own local sources. Verify server gets everything.

  //4. Client sends a MM request. On the server we send the first to another server, consume one, send the next one down,
  //and alternate. Verify the server gets what we need and our local server gets what we need.

  //5. Client sends a MM request to a server. Server sends a request to another server and gets back a MM response.
  //We now have two readers. It consumes the first part from the second one and sends the original MM, a local input stream
  //and and the 2nd part from the second MM to a third server. The second server then complete drains the remaining bits from the 2nd MM.
  //Assert everyone got what they wanted.






}
