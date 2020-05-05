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

package com.symphony.oss.fugue.pubsub;

import com.symphony.oss.fugue.counter.ITopicBusyCounterFactory;

/**
 * A builder for a pull subscriber manager of payload type P.
 * 
 * @author Bruce Skingle
 *
 * @param <T> Type of concrete builder, needed for fluent methods.
 * @param <P> Type of the payload.
 * @param <B> Type of concrete manager (built object), needed for fluent methods.
 */
public interface IPullSubscriberManagerBuilder<T extends IPullSubscriberManagerBuilder<T,P,B>, P, B extends ISubscriberManager>
extends ISubscriberManagerBuilder<T,P,B>
{
  /**
   * Set the ITopicBusyCounterFactory to use.
   * 
   * @param busyCounterFactory An ITopicBusyCounterFactory to use.
   * 
   * @return this (fluent method)
   */
  T withBusyCounterFactory(ITopicBusyCounterFactory busyCounterFactory);

}
