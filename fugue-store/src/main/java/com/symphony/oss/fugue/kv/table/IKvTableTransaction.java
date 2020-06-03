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

package com.symphony.oss.fugue.kv.table;

import java.util.Collection;
import java.util.Set;

import com.symphony.oss.commons.hash.Hash;
import com.symphony.oss.fugue.kv.IKvItem;
import com.symphony.oss.fugue.kv.IKvPartitionSortKeyProvider;
import com.symphony.oss.fugue.store.TransactionFailedException;
import com.symphony.oss.fugue.trace.ITraceContext;

/**
 * Low level storage of KV Items.
 * 
 * @author Bruce Skingle
 *
 */
public interface IKvTableTransaction
{
  /**
   * Store the given collection of items, checking that the given ppartition sort key pair does not already exist.
   * 
   * @param partitionSortKeyProvider  The partition and sort key of the item.
   * @param kvItems Items to be stored.
   */
  void store(IKvPartitionSortKeyProvider partitionSortKeyProvider, Collection<IKvItem> kvItems);
  

  /**
   * Update an existing object.
   * 
   * @param partitionSortKeyProvider  The partition and sort key of the existing item.
   * @param absoluteHash    The absolute hash of the existing item.
   * @param kvItems         A set of items to be put.
   */
  void update(IKvPartitionSortKeyProvider partitionSortKeyProvider, Hash absoluteHash, Set<IKvItem> kvItems);


  void commit(ITraceContext trace) throws TransactionFailedException;
  
}
