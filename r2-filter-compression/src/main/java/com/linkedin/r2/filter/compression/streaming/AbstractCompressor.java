/*
   Copyright (c) 2015 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package com.linkedin.r2.filter.compression.streaming;

import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;


/**
 * @author Ang Xu
 */
public abstract class AbstractCompressor implements StreamingCompressor
{
  @Override
  public EntityStream inflate(EntityStream input)
  {
    StreamingInflater inflater = createInflater();
    input.setReader(inflater);
    return EntityStreams.newEntityStream(inflater);
  }

  @Override
  public EntityStream deflate(EntityStream input)
  {
    StreamingDeflater deflater = createDeflater();
    input.setReader(deflater);
    return EntityStreams.newEntityStream(deflater);
  }

  abstract protected StreamingInflater createInflater();
  abstract protected StreamingDeflater createDeflater();
}
