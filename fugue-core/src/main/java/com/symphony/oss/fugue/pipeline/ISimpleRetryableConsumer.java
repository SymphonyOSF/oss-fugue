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

import javax.annotation.concurrent.NotThreadSafe;

import com.symphony.oss.fugue.trace.ITraceContext;

/**
 * A consumer of some payload which supports retries.
 * 
 * Implementations of this interface may, or may not, be thread
 * safe. Implementations which <i>are</i> thread safe should
 * implement {@link IThreadSafeRetryableConsumer}.
 * 
 * Callers <b>MUST NOT</b> call methods on this interface concurrently
 * from multiple threads, they <b>MUST</b> require an {@link IThreadSafeConsumer}
 * to do so.
 * 
 * @author Bruce Skingle
 *
 * @param <T> The type of payload consumed.
 */
@NotThreadSafe
@FunctionalInterface
public interface ISimpleRetryableConsumer<T>
{
  /**
   * Consume the given item.
   * 
   * A normal return from this method indicates that the item has been fully processed,
   * and the provider can discard the item. In the event that the item cannot be
   * processed then the implementation must throw some kind of Exception.
   * 
   * @param item The item to be consumed.
   * @param trace A trace context.
   * 
   * @throws RetryableConsumerException If the consumer failed to process the item but
   * a retry might be successful. The exception includes a retry time which is the 
   * delay which the thrower considers would be appropriate before any retry.
   * 
   * There is no guarantee that the caller will make any further call or that it will
   * wait for the indicated retryTime, this is merely a hint.
   * 
   * @throws FatalConsumerException If the consumer failed to process the item and
   * a retry is unlikely to be be successful. 
   */
  void consume(T item, ITraceContext trace) throws RetryableConsumerException, FatalConsumerException;
}
