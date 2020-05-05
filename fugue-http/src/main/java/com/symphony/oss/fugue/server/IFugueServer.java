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

package com.symphony.oss.fugue.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import com.symphony.oss.fugue.Fugue;
import com.symphony.oss.fugue.container.IFugeComponentContainer;

/**
 * The main component for a Fugue process.
 * 
 * @author Bruce Skingle
 *
 */
public interface IFugueServer extends IFugeComponentContainer
{
  /**
   * Create a new ExecutorService with a thread pool using the given name.
   * 
   * If tasks submitted to this service throw exceptions then they are handled the Fugue way.
   * 
   * The ExecutorService will be shut down when the server terminates.
   * 
   * @param name The name for threads in the executor.
   * 
   * @return A new ExecutorService.
   */
  ExecutorService newExecutor(String name);

  /**
   * Create a new ExecutorService using the given underlying service.
   * 
   * If tasks submitted to this service throw exceptions then they are handled the Fugue way.
   * 
   * The ExecutorService will be shut down when the server terminates.
   * 
   * @param exec  An executor to so the actual work.
   * 
   * @return A new ExecutorService.
   */
  ExecutorService newExecutor(ExecutorService exec);

  /**
   * Create a new ScheduledExecutorService with a thread pool using the given name.
   * 
   * If tasks submitted to this service throw exceptions then they are handled the Fugue way.
   * 
   * The ExecutorService will be shut down when the server terminates.
   * 
   * @param name The name for threads in the executor.
   * 
   * @return A new ScheduledExecutorService.
   */
  ScheduledExecutorService newScheduledExecutor(String name);

  /**
   * Create a new ScheduledExecutorService using the given underlying service.
   * 
   * If tasks submitted to this service throw exceptions then they are handled the Fugue way.
   * 
   * The ExecutorService will be shut down when the server terminates.
   * 
   * @param exec  An executor to so the actual work.
   * 
   * @return A new ScheduledExecutorService.
   */
  ScheduledExecutorService newScheduledExecutor(ScheduledExecutorService exec);

  /**
   * Return the instanceId for this process.
   * 
   * @return the instanceId.
   */
  static String getInstanceId()
  {
    String instanceId = System.getProperty(Fugue.FUGUE_INSTANCE);
    
    if(instanceId == null)
      instanceId = System.getenv(Fugue.FUGUE_INSTANCE);
    
    if(instanceId == null)
      return "UNKNOWN";
    else
      return instanceId;
  }
  
  /**
   * 
   * @return The application name.
   */
  String getApplicationName();


}