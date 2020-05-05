/*
 * Copyright 2019 Symphony Communication Services, LLC.
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

package com.symphony.oss.fugue.store;

import java.time.Instant;

import javax.annotation.Nullable;

/**
 * The payload of an IFugueObject.
 * 
 * @author Bruce Skingle
 *
 */
public interface IFugueObjectPayload
{

  /**
   * Return a short textual description of this object.
   * 
   * Open objects should indicate their internal type, for example an OpenBlob should indicate the
   * type of the enclosed application payload.
   * 
   * @return a short textual description of this object.
   */
  String getDescription();

  /**
   * 
   * @return The pod which owns this object, if any.
   */
  @Nullable IFuguePodId getPodId();

  /**
   * 
   * @return The type id of the payload in this object, if any.
   */
  @Nullable String getPayloadType();

  /**
   * 
   * @return The purge date for this object, if any.
   */
  @Nullable Instant getPurgeDate();
}
