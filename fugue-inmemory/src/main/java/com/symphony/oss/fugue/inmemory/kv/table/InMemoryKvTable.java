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

package com.symphony.oss.fugue.inmemory.kv.table;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.symphony.oss.commons.fault.FaultAccumulator;
import com.symphony.oss.commons.fluent.BaseAbstractBuilder;
import com.symphony.oss.commons.hash.Hash;
import com.symphony.oss.fugue.kv.IKvItem;
import com.symphony.oss.fugue.kv.IKvPagination;
import com.symphony.oss.fugue.kv.IKvPartitionKeyProvider;
import com.symphony.oss.fugue.kv.IKvPartitionSortKeyProvider;
import com.symphony.oss.fugue.kv.KvCondition;
import com.symphony.oss.fugue.kv.KvPagination;
import com.symphony.oss.fugue.kv.table.IKvTable;
import com.symphony.oss.fugue.store.NoSuchObjectException;
import com.symphony.oss.fugue.store.ObjectExistsException;
import com.symphony.oss.fugue.trace.ITraceContext;

/**
 * Base implementation of IKvTable.
 * 
 * @author Bruce Skingle
 */
public class InMemoryKvTable implements IKvTable
{
  protected static final String  Separator = "#";
  
  /** The serviceId forms part of the partition key for all values in this table. */
  protected final String         serviceId_;

  private final Map<String, TreeMap<String, IKvItem>>  partitionMap_ = new HashMap<>();
  
  protected InMemoryKvTable(AbstractBuilder<?,?> builder)
  {
    serviceId_    = builder.serviceId_;
  }

  @Override
  public void start()
  {
  }

  @Override
  public void stop()
  {
  }

  @Override
  public void deleteRow(IKvPartitionSortKeyProvider partitionSortKeyProvider, ITraceContext trace)
  {
    String partitionKey = getPartitionKey(partitionSortKeyProvider);
    String sortKey = partitionSortKeyProvider.getSortKey().asString();
    
    Map<String, IKvItem> partition = getPartition(partitionKey);
    
    synchronized (partition)
    {
      partition.remove(sortKey);
    }
  }

  @Override
  public void delete(IKvPartitionSortKeyProvider partitionSortKeyProvider, Hash absoluteHash,
      IKvPartitionKeyProvider versionPartitionKey, IKvPartitionSortKeyProvider absoluteHashPrefix, ITraceContext trace)
      throws NoSuchObjectException
  {
    String partitionKey = getPartitionKey(partitionSortKeyProvider);
    String sortKey = partitionSortKeyProvider.getSortKey().asString();
    
    Map<String, IKvItem> partition = getPartition(partitionKey);
    
    synchronized (partition)
    {
      IKvItem existing = partition.get(sortKey);
      
      if(existing == null)
        throw new NoSuchObjectException("Object does not exist");
      
      if(!absoluteHash.equals(existing.getAbsoluteHash()))
        throw new NoSuchObjectException("Object has changed");
      
      partition.remove(sortKey);
    }
    
    partition = getPartition(getPartitionKey(versionPartitionKey));
    
    synchronized (partition)
    {
      for(IKvItem item : partition.values())
      {
        Hash ah = item.getAbsoluteHash();
        String pk = getPartitionKey(absoluteHashPrefix) + ah;
        String sk = absoluteHashPrefix.getSortKey().asString();
        
        TreeMap<String, IKvItem> p2 = getPartition(pk);
        
        p2.remove(sk);
      }
      
      partition.clear();
    }
  }

  @Override
  public void update(IKvPartitionSortKeyProvider partitionSortKeyProvider, Hash absoluteHash, Set<IKvItem> kvItems,
      ITraceContext trace) throws NoSuchObjectException
  {
    String partitionKey = getPartitionKey(partitionSortKeyProvider);
    String sortKey = partitionSortKeyProvider.getSortKey().asString();
    
    Map<String, IKvItem> partition = getPartition(partitionKey);
    
    synchronized (partition)
    {
      IKvItem existing = partition.get(sortKey);
      
      if(existing == null)
        throw new NoSuchObjectException("Object does not exist");
      
      if(!absoluteHash.equals(existing.getAbsoluteHash()))
        throw new NoSuchObjectException("Object has changed");
      
      for(IKvItem kvItem : kvItems)
      {
        String updatePartitionKey = getPartitionKey(kvItem);
        
        Map<String, IKvItem> updatePartition = getPartition(updatePartitionKey);
        
        if(partition == updatePartition && !sortKey.equals(kvItem.getSortKey().asString()))
        {
          if(updatePartition.containsKey(sortKey))
            throw new NoSuchObjectException("An object with the new sort key already exists.");
        }
      }
      
      partition.remove(sortKey);
      
      for(IKvItem kvItem : kvItems)
      {
        String updatePartitionKey = getPartitionKey(kvItem);
        
        Map<String, IKvItem> updatePartition = getPartition(updatePartitionKey);
        
        updatePartition.put(kvItem.getSortKey().asString(), kvItem);
      }
    }
  }

  private void store(IKvItem kvItem)
  { 
    String partitionKey = getPartitionKey(kvItem);
    String sortKey = kvItem.getSortKey().asString();
    
    Map<String, IKvItem> partition = getPartition(partitionKey);
    
    partition.put(sortKey, kvItem);
  }

  @Override
  public synchronized void store(IKvItem kvItem, KvCondition kvCondition, ITraceContext trace)
  {
    String partitionKey = getPartitionKey(kvItem);
    String sortKey = kvItem.getSortKey().asString();
    
    Map<String, IKvItem> partition = getPartition(partitionKey);
    IKvItem existingItem = partition.get(sortKey);
    
    if(existingItem != null)
    {
      Object value = existingItem.getAdditionalAttributes().get(kvCondition.getName());
      
      if(value == null)
        return;
      
      switch(kvCondition.getComparison())
      {
        case EQUALS:
          if(!kvCondition.getValue().equals(value.toString()))
            return;
          break;
          
        case GREATER_THAN:
          if(kvCondition.getValue().compareTo(value.toString()) >= 0)
            return;
          break;
          
        case LESS_THAN:
          if(kvCondition.getValue().compareTo(value.toString()) <= 0)
            return;
          break;
      }
    }
    
    partition.put(sortKey, kvItem);
  }

  private synchronized TreeMap<String, IKvItem> getPartition(String partitionKey)
  {
    TreeMap<String, IKvItem> partition = partitionMap_.get(partitionKey);
    
    if(partition == null)
    {
      partition = new TreeMap<>();
      partitionMap_.put(partitionKey, partition);
    }
    
    return partition;
  }

  private String getPartitionKey(IKvPartitionKeyProvider kvItem)
  {
    return serviceId_ + Separator + kvItem.getPartitionKey();
  }

  @Override
  public void store(Collection<IKvItem> kvItems, ITraceContext trace)
  {
    for(IKvItem item : kvItems)
      store(item);
  }

  @Override
  public void store(IKvPartitionSortKeyProvider partitionSortKeyProvider, Collection<IKvItem> kvItems,
      ITraceContext trace) throws ObjectExistsException
  {
    if(partitionSortKeyProvider != null)
    {
      String partitionKey = getPartitionKey(partitionSortKeyProvider);
      String sortKey = partitionSortKeyProvider.getSortKey().asString();
      
      Map<String, IKvItem> partition = getPartition(partitionKey);
      
      if(partition.containsKey(sortKey))
          throw new ObjectExistsException("Object with key " + partitionSortKeyProvider + " already exists.");
    }
    
    for(IKvItem item : kvItems)
      store(item);
  }

  @Override
  public String fetch(IKvPartitionSortKeyProvider partitionSortKey, ITraceContext trace) throws NoSuchObjectException
  {
    String partitionKey = getPartitionKey(partitionSortKey);
    String sortKey = partitionSortKey.getSortKey().asString();
    
    TreeMap<String, IKvItem> partition = getPartition(partitionKey);
    
    IKvItem item = partition.get(sortKey);
    
    if(item == null)
      throw new NoSuchObjectException();
    
    return item.getJson();
  }

  @Override
  public String fetchFirst(IKvPartitionKeyProvider partitionKeyProvider, ITraceContext trace) throws NoSuchObjectException
  {
    String partitionKey = getPartitionKey(partitionKeyProvider);
    
    TreeMap<String, IKvItem> partition = getPartition(partitionKey);
    
    IKvItem item = partition.isEmpty() ? null : partition.firstEntry().getValue();
    
    if(item == null)
      throw new NoSuchObjectException();
    
    return item.getJson();
  }

  @Override
  public String fetchLast(IKvPartitionKeyProvider partitionKeyProvider, ITraceContext trace) throws NoSuchObjectException
  {
    String partitionKey = getPartitionKey(partitionKeyProvider);
    
    TreeMap<String, IKvItem> partition = getPartition(partitionKey);
    
    IKvItem item = partition.isEmpty() ? null : partition.lastEntry().getValue();
    
    if(item == null)
      throw new NoSuchObjectException();
    
    return item.getJson();
  }

  @Override
  public void createTable(boolean dryRun)
  {
  }

  @Override
  public void deleteTable(boolean dryRun)
  {
  }

  @Override
  public IKvPagination fetchPartitionObjects(IKvPartitionKeyProvider partitionKeyProvider, boolean scanForwards, Integer limit,
      String after, String sortKeyPrefix,
      @Nullable Map<String, Object> filterAttributes, Consumer<String> consumer, ITraceContext trace)
  {
    String partitionKey = getPartitionKey(partitionKeyProvider);
    
    TreeMap<String, IKvItem> partition = getPartition(partitionKey);

    NavigableMap<String, IKvItem> map; 
    String before = null;
    
    if(after == null)
    {
      if(scanForwards)
        map = partition;
      else
        map = partition.descendingMap();
    }
    else if(scanForwards)
    {
      map   = partition.tailMap(after, false);
      before = map.isEmpty() ||  map.firstKey().equals(partition.firstKey()) ? null : map.firstKey();
    }
    else
    {
      map   = partition.descendingMap().tailMap(after, false);
      before = map.isEmpty() ||  map.firstKey().equals(partition.firstKey()) ? null : map.firstKey();
    }
    
    if(limit == null)
      limit = 100;
    
    int available = map.entrySet().size();
    
    for(Entry<String, IKvItem> entry : map.entrySet())
    {
      boolean ok = (sortKeyPrefix == null || entry.getKey().startsWith(sortKeyPrefix));
      
      if(ok && filterAttributes != null)
      {
        for(Entry<String, Object> attr : filterAttributes.entrySet())
        {
          Object rowAttr = entry.getValue().getAdditionalAttributes().get(attr.getKey());
          
          if(!attr.getValue().equals(rowAttr))
          {
            ok = false;
            break;
          }
        }
      }
      
      available--;
      
      if(ok)
      {
        consumer.accept(entry.getValue().getJson());
      
        if(--limit <= 0)
          return new KvPagination(before, available==0 ? null : entry.getKey());
      }
    }
        
    return new KvPagination(before, null);
  }
  
  /**
   * Builder.
   * 
   * @author Bruce Skingle
   *
   */
  public static class Builder extends AbstractBuilder<Builder, InMemoryKvTable>
  {
    /**
     * Constructor.
     */
    public Builder()
    {
      super(Builder.class);
    }

    @Override
    protected InMemoryKvTable construct()
    {
      return new InMemoryKvTable(this);
    }
  }

  protected static abstract class AbstractBuilder<T extends AbstractBuilder<T,B>, B extends InMemoryKvTable> extends BaseAbstractBuilder<T,B>
  {
    protected String         serviceId_;
    
    protected AbstractBuilder(Class<T> type)
    {
      super(type);
    }
    
    @Override
    public void validate(FaultAccumulator faultAccumulator)
    {
      super.validate(faultAccumulator);
      
      faultAccumulator.checkNotNull(serviceId_,   "serviceId");
    }

    /**
     * The serviceId forms part of the partition key for all values in this table.
     * 
     * @param serviceId The serviceId for this table.
     * 
     * @return This (fluent method).
     */
    public T withServiceId(String serviceId)
    {
      serviceId_ = serviceId;
      
      return self();
    }
  }
}
