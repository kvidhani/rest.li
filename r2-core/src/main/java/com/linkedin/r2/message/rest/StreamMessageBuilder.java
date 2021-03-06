package com.linkedin.r2.message.rest;

import com.linkedin.r2.message.streaming.EntityStream;

/**
 * @author Zhenkai Zhu
 */
public interface StreamMessageBuilder<B extends StreamMessageBuilder<B>>
{

  /**
   * Constructs an {@link StreamMessage} using the settings configured in this builder and the supplied EntityStream.
   *
   * @param stream the entity stream for this message
   * @return a Stream from the settings in this builder and the supplied EntityStream
   */
  StreamMessage build(EntityStream stream);

  /**
   * Similar to {@link #build}, but the returned Message is in canonical form.
   *
   * @param stream the entity stream for this message
   * @return a Stream from the settings in this builder and the supplied EntityStream
   */
  StreamMessage buildCanonical(EntityStream stream);
}
