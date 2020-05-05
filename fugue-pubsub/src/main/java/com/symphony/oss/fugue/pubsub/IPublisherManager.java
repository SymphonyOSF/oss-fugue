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

import com.symphony.oss.fugue.naming.TopicName;

/**
 * A pub/sub publisher manager.
 * 
 * @author Bruce Skingle
 */
public interface IPublisherManager
{
  /**
   * Get the IPublisher for the given named topic.
   * 
   * @param topicId The actual name of a topic.
   * 
   * @return The publisher for the required topic.
   */
  IPublisher getPublisherByName(String topicId);

  /**
   * Get the IPublisher for the given named topic.
   * 
   * @param serviceId The ID of the service which owns the topic.
   * @param topicId   The actual name of a topic.
   * 
   * @return The publisher for the required topic.
   */
  IPublisher getPublisherByName(String serviceId, String topicId);

  /**
   * Get the IPublisher for the given named topic.
   * 
   * @param topicName   The name of a topic.
   * 
   * @return The publisher for the required topic.
   */
  IPublisher getPublisherByName(TopicName topicName);
  
  /**
   * Return the maximum allowed size of a message in bytes.
   * 
   * @return The maximum allowed size of a message in bytes.
   */
  int getMaximumMessageSize();
}
