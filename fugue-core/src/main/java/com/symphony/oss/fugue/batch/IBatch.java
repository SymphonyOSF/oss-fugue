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

package com.symphony.oss.fugue.batch;

import java.util.Collection;

/**
 * A batch of tasks which can be executed in parallel.
 * 
 * @param <T> The type of tasks in the batch
 * 
 * @author Bruce Skingle
 *
 */
public interface IBatch<T extends Runnable>
{
  /**
   * Submit the given task.
   * 
   * @param task Some task to be executed as part of the batch.
   */
  public void submit(T task);
  
  /**
   * Block until all tasks have completed.
   */
  public void waitForAllTasks();

  /**
   * Block until all tasks have completed or the given timeout expires.
   * 
   * @param timeoutMillis Timeout in milliseconds.
   * @return A copy of the collection of incomplete tasks in the batch.
   */
  Collection<T> waitForAllTasks(long timeoutMillis);
}
