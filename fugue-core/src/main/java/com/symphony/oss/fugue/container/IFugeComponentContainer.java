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

package com.symphony.oss.fugue.container;

import java.util.List;

import com.symphony.oss.fugue.IFugueComponent;
import com.symphony.oss.fugue.IFugueLifecycleComponent;

/**
 * A fluent container of Fugue components.
 * 
 * @author Bruce Skingle
 *
 */
public interface IFugeComponentContainer
{
  /**
   * @return all of the registered components which implement IFugueComponent.
   */
  List<IFugueComponent> getComponents();

  /**
   * @return all of the registered components which implement IFugueLifecycleComponent.
   */
  List<IFugueLifecycleComponent> getLifecycleComponents();

  /**
   * Set the running state of the container.
   * 
   * @param running the new state.
   * 
   * @return the previous state.
   */
  boolean setRunning(boolean running);

  /**
   * Return true iff the container is running.
   * 
   * Threads may call this method in their main loop to determine if they should terminate.
   * 
   * @return true iff the server is running.
   */
  boolean isRunning();

}
