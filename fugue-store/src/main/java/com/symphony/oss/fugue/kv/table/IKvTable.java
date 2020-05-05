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
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.symphony.oss.commons.hash.Hash;
import com.symphony.oss.fugue.IFugueComponent;
import com.symphony.oss.fugue.kv.IKvItem;
import com.symphony.oss.fugue.kv.IKvPagination;
import com.symphony.oss.fugue.kv.IKvPartitionKeyProvider;
import com.symphony.oss.fugue.kv.IKvPartitionSortKeyProvider;
import com.symphony.oss.fugue.kv.KvCondition;
import com.symphony.oss.fugue.store.NoSuchObjectException;
import com.symphony.oss.fugue.store.ObjectExistsException;
import com.symphony.oss.fugue.trace.ITraceContext;

/**
 * Low level storage of KV Items.
 * 
 * @author Bruce Skingle
 *
 */
public interface IKvTable extends IFugueComponent
{
  /**
   * Store the given collection of items, overwriting any existing object with the same partition and sort keys.
   * 
   * @param kvItems Items to be stored.
   * @param trace   Trace context.
   */
  void store(Collection<IKvItem> kvItems, ITraceContext trace);
  
  /**
   * Store the given item, provided the given condition is met.
   * 
   * @param kvItem      Item to be stored.
   * @param kvCondition Condition.
   * @param trace       Trace context.
   */
  void store(IKvItem kvItem, KvCondition kvCondition, ITraceContext trace);
  

  /**
   * Store the given collection of items, checking that the given ppartition sort key pair does not already exist.
   * 
   * @param partitionSortKeyProvider  The partition and sort key of the item.
   * @param kvItems Items to be stored.
   * @param trace   Trace context.
   * 
   * @throws ObjectExistsException If an object with the give partition and sort key already exists. 
   */
  void store(IKvPartitionSortKeyProvider partitionSortKeyProvider, Collection<IKvItem> kvItems, ITraceContext trace) throws ObjectExistsException;
  

  /**
   * Update an existing object.
   * 
   * @param partitionSortKeyProvider  The partition and sort key of the existing item.
   * @param absoluteHash    The absolute hash of the existing item.
   * @param kvItems         A set of items to be put.
   * @param trace           Trace context.
   * 
   * @throws NoSuchObjectException If the object to be updated has changed. 
   */
  void update(IKvPartitionSortKeyProvider partitionSortKeyProvider, Hash absoluteHash, Set<IKvItem> kvItems,
      ITraceContext trace) throws NoSuchObjectException;
  
  /**
   * Delete the given object.
   * 
   * @param partitionSortKeyProvider  The partition and sort key of the existing item.
   * @param absoluteHash              The absolute hash of the existing item.
   * @param versionPartitionKey       Partition key for the versions partition.
   * @param absoluteHashPrefix        Prefix for Absolute records to delete
   * @param trace                     Trace context.
   * 
   * @throws NoSuchObjectException If the object to be deleted has changed. 
   */
  void delete(IKvPartitionSortKeyProvider partitionSortKeyProvider, Hash absoluteHash, IKvPartitionKeyProvider versionPartitionKey,
      IKvPartitionSortKeyProvider absoluteHashPrefix, ITraceContext trace) throws NoSuchObjectException;
  
  /**
   * Fetch the object with the given partition key and sort key.
   * 
   * @param partitionSortKey  The key of the required object.
   * @param trace             Trace context.
   * 
   * @return                  The required object.
   * 
   * @throws NoSuchObjectException  If there is no object with the given baseHash.
   */
  String fetch(IKvPartitionSortKeyProvider partitionSortKey, ITraceContext trace) throws NoSuchObjectException;
  
  /**
   * Fetch the first object with the given partition key.
   * 
   * @param partitionKey    The partition key of the required object.
   * @param trace           Trace context.
   * 
   * @return                The required object.
   * 
   * @throws NoSuchObjectException  If there is no object with the given partition key.
   */
  String fetchFirst(IKvPartitionKeyProvider partitionKey, ITraceContext trace) throws NoSuchObjectException;

  /**
   * Fetch the last object with the given partition key.
   * 
   * @param partitionKey    The partition key of the required object.
   * @param trace           Trace context.
   * 
   * @return                The required object.
   * 
   * @throws NoSuchObjectException  If there is no object with the given partition key.
   */
  String fetchLast(IKvPartitionKeyProvider partitionKey, ITraceContext trace) throws NoSuchObjectException;

  /**
   * Create the table.
   * 
   * @param dryRun If true then no changes are made but log messages show what would happen.
   */
  void createTable(boolean dryRun);

  /**
   * Delete the table.
   * 
   * @param dryRun If true then no changes are made but log messages show what would happen.
   */
  void deleteTable(boolean dryRun);

  /**
   * Return objects from the given partition.
   * 
   * @param partitionKey      The ID of the partition.
   * @param scanForwards      If true then scan objects in the order of their sort keys, else in reverse order.
   * @param limit             An optional limit to the number of objects retrieved.
   * @param after             An optional page cursor to continue a previous query.
   * @param sortKeyPrefix     An optional sort key prefix.
   * @param filterAttributes  Optional attribute values to filter results.
   * @param consumer          A consumer to receive the retrieved objects.
   * @param trace             Trace context.
   * 
   * @return              Pagination tokens to allow a continuation query to be made.
   */
  IKvPagination fetchPartitionObjects(IKvPartitionKeyProvider partitionKey, boolean scanForwards, Integer limit, 
      @Nullable String after,
      @Nullable String sortKeyPrefix,
      @Nullable Map<String, Object> filterAttributes,
      Consumer<String> consumer, ITraceContext trace);

  /**
   * Delete the single row whose primary key is given.
   * 
   * @param partitionSortKeyProvider  The partition and sort key of the row to be deleted.
   * @param trace                     Trace context.
   */
  void deleteRow(IKvPartitionSortKeyProvider partitionSortKeyProvider, ITraceContext trace);


}
