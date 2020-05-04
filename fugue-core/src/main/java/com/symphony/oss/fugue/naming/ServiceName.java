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

package com.symphony.oss.fugue.naming;

import javax.annotation.Nonnull;

/**
 * A service name.
 * 
 * @author Bruce Skingle
 *
 */
public class ServiceName extends Name
{
  private final String  serviceId_;
  private final String  podName_;
  private final Integer podId_;

  /**
   * Constructor.
   * 
   * @param serviceId       The ID of the service.
   * @param podName         The physical podName (simple name).
   * @param podId           The logical podId.
   * @param name            The first element of the actual name.
   * @param additional      Zero or more optional suffix elements.
   */
  protected ServiceName(String serviceId, String podName, Integer podId, @Nonnull String name, Object ...additional)
  {
    super(name, additional);
    
    serviceId_ = serviceId;
    podName_ = podName;
    podId_ = podId;
  }

  /**
   * 
   * @return The id (simple name) of the service.
   */
  public String getServiceId()
  {
    return serviceId_;
  }
  
  /**
   * 
   * @return The podName (physical name)
   */
  public String getPodName()
  {
    return podName_;
  }

  /**
   * 
   * @return The logical podId.
   */
  public Integer getPodId()
  {
    return podId_;
  }
}
