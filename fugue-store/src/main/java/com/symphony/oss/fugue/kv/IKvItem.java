/*
 *
 *
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

package com.symphony.oss.fugue.kv;

import java.time.Instant;
import java.util.Map;

import javax.annotation.Nullable;

import com.symphony.oss.commons.hash.Hash;
import com.symphony.oss.fugue.store.IFuguePodId;
import com.symphony.oss.fugue.trace.ITraceSubject;

/**
 * An item to be stored in a KvStore or KvTable.
 * 
 * @author Bruce Skingle
 *
 */
public interface IKvItem extends IKvPartitionSortKeyProvider, ITraceSubject
{
  /**
   * 
   * @return The serialized form of this item.
   */
  String getJson();

  /**
   * 
   * @return The type id of the payload in this object, if any.
   */
  @Nullable String getType();

  /**
   * 
   * @return The purge date for this object, if any.
   */
  @Nullable Instant getPurgeDate();
  
  /**
   * 
   * @return true if this item should be stored to secondary storage.
   */
  boolean isSaveToSecondaryStorage();
  
  /**
   * 
   * @return The absolute hash for this object.
   */
  Hash getAbsoluteHash();
  
  /**
   * 
   * @return The pod which owns this object, if any.
   */
  @Nullable IFuguePodId getPodId();
  
  /**
   * Additional attributes are added to the index record when stored. 
   * Their keys must be between 3 and 10 characters long.
   * 
   * @return Additional attributes.
   */
  default @Nullable Map<String, Object> getAdditionalAttributes()
  {
    return null;
  }
}
