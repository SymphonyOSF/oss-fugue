/*
 *
 *
 * Copyright 2018 Symphony Communication Services, LLC.
 *
 * Licensed to The Symphony Software Foundation (SSF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.symphony.oss.fugue.pipeline;

import com.symphony.oss.commons.immutable.ImmutableByteArray;
import com.symphony.oss.fugue.trace.ITraceContext;

/**
 * Adapts a consumer of ImmutableByteArray for use by classes requiring a
 * consumer of String.
 * 
 * The String payload received is encoded as UTF-8
 * 
 * @author Bruce Skingle
 *
 */
public class StringToByteArrayConsumerAdaptor implements IThreadSafeConsumer<String>
{
  private final IThreadSafeConsumer<ImmutableByteArray> consumer_;

  /**
   * Constructor.
   * 
   * @param consumer A consumer of ImmutableByteArray which will be called when we are called with a String.
   */
  public StringToByteArrayConsumerAdaptor(IThreadSafeConsumer<ImmutableByteArray> consumer)
  {
    consumer_ = consumer;
  }

  @Override
  public void close()
  {
    consumer_.close();
  }

  @Override
  public void consume(String item, ITraceContext trace)
  {
    consumer_.consume(ImmutableByteArray.newInstance(item), trace);
  }
}
