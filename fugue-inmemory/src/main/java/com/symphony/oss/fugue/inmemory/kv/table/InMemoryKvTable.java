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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.symphony.oss.commons.fault.CodingFault;
import com.symphony.oss.commons.fault.FaultAccumulator;
import com.symphony.oss.commons.fluent.BaseAbstractBuilder;
import com.symphony.oss.commons.hash.Hash;
import com.symphony.oss.fugue.kv.IKvItem;
import com.symphony.oss.fugue.kv.IKvPagination;
import com.symphony.oss.fugue.kv.IKvPartitionKeyProvider;
import com.symphony.oss.fugue.kv.IKvPartitionSortKeyProvider;
import com.symphony.oss.fugue.kv.KvCondition;
import com.symphony.oss.fugue.kv.KvPagination;
import com.symphony.oss.fugue.kv.KvPartitionUser;
import com.symphony.oss.fugue.kv.table.IKvTable;
import com.symphony.oss.fugue.kv.table.IKvTableTransaction;
import com.symphony.oss.fugue.pubsub.IPubSubMessage;
import com.symphony.oss.fugue.pubsub.IQueueManager;
import com.symphony.oss.fugue.pubsub.IQueueMessage;
import com.symphony.oss.fugue.pubsub.IQueueMessageDelete;
import com.symphony.oss.fugue.pubsub.IQueueMessageExtend;
import com.symphony.oss.fugue.pubsub.IQueueReceiver;
import com.symphony.oss.fugue.pubsub.IQueueSender;
import com.symphony.oss.fugue.pubsub.QueueNotFoundException;
import com.symphony.oss.fugue.store.NoSuchObjectException;
import com.symphony.oss.fugue.store.TransactionFailedException;
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
  private final Map<String,   Deque<IQueueMessage>>    queueMap_     = new HashMap<>();
  
  private static final String KEY_EXISTS = "An object with given partition and sort key already exists.";
  private static final String KEY_EXISTS_OR_OBJECT_CHANGED = "An object with given partition and sort key already exists, or the object to be updated has changed.";
 
  private static final Logger                log_ = LoggerFactory.getLogger(InMemoryKvTable.class);
  
  protected InMemoryKvTable(AbstractBuilder<?,?> builder)
  {
    serviceId_    = builder.serviceId_;
  }
  
  private IQueueManager queueManager_;
  
  public IQueueManager getQueueManager() {
	  
	  queueManager_ = queueManager_ == null ? new InMemoryQueueManager() : queueManager_;
	  
	  return queueManager_;
  }
  
  public class InMemoryQueueMessage implements IQueueMessage
  {

	private String payload;

	public InMemoryQueueMessage(IPubSubMessage msg) 
	{
		payload = msg.getPayload();
	}
	  
	@Override
	public String getReceiptHandle()
	{
		return null;
	}

	@Override
	public String getMessageId() 
	{
		return null;
	}

	@Override
	public String getPayload()
	{
		return payload;
	}
	  
  }

	class InMemoryQueueSender implements IQueueSender 
	{

		private String queueName_;

		InMemoryQueueSender(String queueName)
		{
			queueName_ = queueName;
		}

		@Override
		public void sendMessage(IPubSubMessage pubSubMessage) 
		{
			pubSubMessage.getTraceContext().trace("ABOUT-TO-SEND", "SQS_QUEUE", queueName_);

			synchronized(queueMap_)
			{
				queueMap_.get(queueName_).add(new InMemoryQueueMessage(pubSubMessage));
			}
			pubSubMessage.getTraceContext().trace("SENT", "SQS_QUEUE", queueName_);
		}
	}

	class InMemoryQueueReceiver implements IQueueReceiver {

		private String queueName_;

    InMemoryQueueReceiver(String queueName)
    {
      queueName_ = queueName;
    }

		@Override
		public Collection<IQueueMessage> receiveMessages(int maxMessages, int waitTimeSeconds,
				Set<? extends IQueueMessageDelete> ackMessages, Set<? extends IQueueMessageExtend> nakMessages) {
			List<IQueueMessage> messages = new ArrayList<>(maxMessages);

			if (maxMessages > 0)
			{
        synchronized (queueMap_)
        {
          Deque<IQueueMessage> queue = queueMap_.get(queueName_);

          log_.debug("About to receive messages...");

          for (int i = 0; i < maxMessages && queue.size() > 0; i++)
            messages.add(queue.pollFirst());
        }
      }

			log_.debug("Returning " + messages.size() + " messages");
			
			return messages;
		}

	}

	class InMemoryQueueManager implements IQueueManager 
	{
		protected static final int MAX_MESSAGE_SIZE = 256 * 1024; // 256K

		private final LoadingCache<String, InMemoryQueueSender> senderCache_ = CacheBuilder.newBuilder()
				.maximumSize(250).build(new CacheLoader<String, InMemoryQueueSender>() {
					@Override
					public InMemoryQueueSender load(String queueName)
					{
						return new InMemoryQueueSender(queueName);
					}
				});
		private final LoadingCache<String, InMemoryQueueReceiver> receiverCache_ = CacheBuilder.newBuilder()
				.maximumSize(250).build(new CacheLoader<String, InMemoryQueueReceiver>() {
					@Override
					public InMemoryQueueReceiver load(String queueName) 
					{
						return new InMemoryQueueReceiver(queueName);
					}
				});

		@Override
		public boolean doesQueueExist(String queueName) 
		{
			return queueMap_.containsKey(queueName);
		}

		@Override
		public String createQueue(String queueName, Map<String, String> tags, boolean dryRun)
		{
      synchronized (queueMap_)
      {

        if (queueMap_.containsKey(queueName))
        {
          log_.info("Queue " + queueName + " already exists");
        }
        else
        {
          queueMap_.put(queueName, new ArrayDeque<IQueueMessage>());

          log_.info("Created queue " + queueName);
        }
      }
			return queueName;
		}

		@Override
		public void deleteQueue(String queueName, boolean dryRun)
    {
      synchronized (queueMap_)
      {
        if (queueMap_.containsKey(queueName))
        {

          if (dryRun)
          {
            log_.info("Subscription " + queueName + " would be deleted (dry run)");
          }
          else
          {
            queueMap_     .remove(queueName);
            senderCache_  .invalidate(queueName);
            receiverCache_.invalidate(queueName);

            log_.info("Deleted queue " + queueName);
          }
        }
        else
          log_.info("Queue " + queueName + " does not exist.");
      }
		}

    @Override
    public IQueueSender getSender(String queueName)
    {
      try
      {
        return senderCache_.get(queueName);
      }
      catch (ExecutionException e)
      {
        throw new CodingFault("Can't Happen", e);
      }
    }

    @Override
    public IQueueReceiver getReceiver(String queueName) throws QueueNotFoundException
    {
      try
      {
        return receiverCache_.get(queueName);
      }
      catch (ExecutionException e)
      {
        throw new QueueNotFoundException("Queue does not exist", e);

      }
    }

		@Override
		public int getMaximumMessageSize() 
		{
			return MAX_MESSAGE_SIZE;
		}

    @Override
    public long getTTLLowerBound()
    {
      return 1000 * 60;
    }

    @Override
    public long getTTLUpperBound()
    {
      return 1000 * 60 * 60 * 24 * (7 - 2 /*CLEANUP DAYS */);
    }

    @Override
    public String getQueueUrl(String queueName)
    {
      return queueName; //PLACEHOLDER
    }

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
  //  objectStore_.fetchPartitionSubscriptions(storedObject.getPartitionHash().toStringBase64(), new FeedSender(storedObject, queueManager_, trace), trace);
  }

  @Override
  public void delete(IKvPartitionSortKeyProvider partitionSortKeyProvider,
      IKvPartitionKeyProvider versionPartitionKey, IKvPartitionSortKeyProvider absoluteHashPrefix, ITraceContext trace)
  {
    String partitionKey = getPartitionKey(partitionSortKeyProvider);
    String sortKey = partitionSortKeyProvider.getSortKey().asString();
    
    Map<String, IKvItem> partition = getPartition(partitionKey);
    
    synchronized (partition)
    {
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
  public void deleteSystemPartitionObjects(IKvPartitionKeyProvider partitionKeyProvider, ITraceContext trace)
  {
    
    String partitionKey                = partitionKeyProvider.getPartitionKey().asString();
    TreeMap<String, IKvItem> partition = partitionMap_.get(partitionKey);
    
    if(partition != null)  
      synchronized(partition) 
      {
        partition.clear();
        partitionMap_.remove(partitionKey);
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
        case NOT_EQUALS:
          if(kvCondition.getValue().compareTo(value.toString()) == 0)
            return;
          
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
  
  @Override
  public synchronized void storeEntitlementMapping(IKvItem kvItem, KvCondition effective, KvCondition entAction, String action, ITraceContext trace)
  {
    String partitionKey = getPartitionKey(kvItem);
    String sortKey = kvItem.getSortKey().asString();
    
    Map<String, IKvItem> partition = getPartition(partitionKey);
    IKvItem existingItem = partition.get(sortKey);
    
    if(existingItem != null) 
    {
      Object existing_effective_value = existingItem.getAdditionalAttributes().get(effective.getName());
      Object existing_entAction_value = existingItem.getAdditionalAttributes().get(entAction.getName());
      
        if (existing_effective_value.toString().compareTo(effective.getValue()) > 0)
          return;
        if (entAction.toString().equals(existing_entAction_value.toString()))
          if (!entAction.getValue().equals(action))
            return;
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
  public void storeNonTransactional(Collection<IKvItem> kvItems, ITraceContext trace)
  {
    for(IKvItem item : kvItems)
      store(item);
  }
  
  @Override
  public IKvTableTransaction createTransaction()
  {
    return new Transaction();
  }

  class Transaction implements IKvTableTransaction
  {
    private final Map<String, TransactionPartition>  txnPartitionMap_ = new HashMap<>();
    private TransactionFailedException  exception_;
    
    class TransactionPartition
    {
      TreeMap<String, IKvItem> itemMap_ = new TreeMap<>();

      Set<String>              txnSortKeyMap_ = new HashSet<>();
    }

    
    private synchronized TransactionPartition getTxnPartition(String partitionKey)
    {
      TransactionPartition partition = txnPartitionMap_.get(partitionKey);
      
      if(partition == null)
      {
        TreeMap<String, IKvItem> p = getPartition(partitionKey);
        
        partition = new TransactionPartition();
        partition.itemMap_ = new TreeMap<String, IKvItem>(p);
        partition.txnSortKeyMap_.addAll(p.keySet());
        
        txnPartitionMap_.put(partitionKey, partition);
      }
      return partition;
    }
    
    private void txnStore(IKvItem kvItem)
    { 
      String partitionKey = getPartitionKey(kvItem);
      String sortKey = kvItem.getSortKey().asString();
      
      TransactionPartition partition = getTxnPartition(partitionKey);
      
      if(partition.txnSortKeyMap_.contains(sortKey))

      {
        exception_ = new TransactionFailedException("Object with key " + sortKey + " already exists. 1");
      }
      else

      {
        partition.itemMap_.put(sortKey, kvItem);
        partition.txnSortKeyMap_.add(sortKey);
      }
      
    }
    
    @Override
    public void store(IKvPartitionSortKeyProvider partitionSortKeyProvider, Collection<IKvItem> kvItems)
    {
      if(exception_ != null)
        return;
      
      if(partitionSortKeyProvider != null)
      {
        String partitionKey = getPartitionKey(partitionSortKeyProvider);
        String sortKey = partitionSortKeyProvider.getSortKey().asString();
        
        TransactionPartition partition = getTxnPartition(partitionKey);
        
        if(partition.txnSortKeyMap_.contains(sortKey))
        {
          exception_ = new TransactionFailedException("Object with key " + partitionSortKeyProvider + " already exists. 2");
          return;
        }
      }
      
      for(IKvItem item : kvItems)
        txnStore(item);
    }

    @Override
    public void update(IKvPartitionSortKeyProvider partitionSortKeyProvider, Hash absoluteHash, Set<IKvItem> kvItems)
    {
      if(exception_ != null)
        return;
      
      String partitionKey = getPartitionKey(partitionSortKeyProvider);
      String sortKey = partitionSortKeyProvider.getSortKey().asString();
      
      TransactionPartition partition = getTxnPartition(partitionKey);
      
      synchronized (partition)
      {
        IKvItem existing = partition.itemMap_.get(sortKey);
        
        if(existing == null)
        {
          exception_ = new TransactionFailedException("Object does not exist");
          return;
        }
        
        if(!absoluteHash.equals(existing.getAbsoluteHash()))
        {
          exception_ = new TransactionFailedException("Object has changed");
          return;
        }
        
        for(IKvItem kvItem : kvItems)
        {
          String updatePartitionKey = getPartitionKey(kvItem);
          
          Map<String, IKvItem> updatePartition = getPartition(updatePartitionKey);
          
          if(partition == updatePartition && !sortKey.equals(kvItem.getSortKey().asString()))
          {
            if(updatePartition.containsKey(sortKey))
            {
              exception_ = new TransactionFailedException("An object with the new sort key already exists. 3");
              return;
            }
          }
        }
        
        partition.itemMap_.remove(sortKey);
        partition.txnSortKeyMap_.remove(sortKey);
        
        for(IKvItem kvItem : kvItems)
        {
          String updatePartitionKey = getPartitionKey(kvItem);
          
          TransactionPartition updatePartition = getTxnPartition(updatePartitionKey);
          
          String sKey = kvItem.getSortKey().asString();
          
          if(partition.txnSortKeyMap_.contains(sKey))
          {
            exception_ = new TransactionFailedException("Object with key " + sKey + " already exists. 4");
            return;
          }
          
          updatePartition.itemMap_.put(sKey, kvItem);
          updatePartition.txnSortKeyMap_.add(sKey);
         
        }
      }
    }

    @Override
    public void commit(ITraceContext trace) throws TransactionFailedException
    {
      if(exception_ != null)
        throw exception_;
      
      for(Entry<String, TransactionPartition> e : txnPartitionMap_.entrySet())
        partitionMap_.put(e.getKey(), e.getValue().itemMap_);
    }
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
      String after, String sortKeyPrefix, String sortKeyMin, String sortKeyMax,
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
    NavigableSet<String> keys = map.navigableKeySet();
    
    for(Entry<String, IKvItem> entry : map.entrySet())
    {
      boolean ok = checkKey(sortKeyPrefix, sortKeyMin, sortKeyMax, entry.getKey());
      
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
  
  private boolean checkKey(String sortKeyPrefix, String sortKeyMin, String sortKeyMax, String key)
  {
    boolean ret = false;

    if (sortKeyMin != null || sortKeyMax != null)
    {
      ret = (sortKeyMin == null || key.compareTo(sortKeyMin) >= 0)
          && (sortKeyMax == null || key.compareTo(sortKeyMax) <= 0);
    }
    else if (sortKeyPrefix != null)
    {
      ret = key.startsWith(sortKeyPrefix);
    }
    else
      ret = true;

    return ret;
  }
  
  @Override
  public IKvPagination fetchPartitionObjects(IKvPartitionKeyProvider partitionKeyProvider, boolean scanForwards, Integer limit,
      String after, String sortKeyPrefix, String sortKeyMin, String sortKeyMax, Map<String, Object> filterAttributes, BiConsumer<String, String> consumer,
      ITraceContext trace)
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
      boolean ok = checkKey(sortKeyPrefix, sortKeyMin, sortKeyMax, entry.getKey());
      
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
        consumer.accept(entry.getValue().getSortKey().asString(), entry.getValue().getJson());
      
        if(--limit <= 0)
          return new KvPagination(before, available==0 ? null : entry.getKey());
      }
    }
        
    return new KvPagination(before, null);
  }

  
  @Override
  public IKvPagination fetchPartitionUsers(IKvPartitionKeyProvider partitionKeyProvider, Integer limit,
	      String after, Consumer<KvPartitionUser> consumer, ITraceContext trace)
  {
	    String partitionKey = getPartitionKey(partitionKeyProvider);
	    
	    TreeMap<String, IKvItem> partition = getPartition(partitionKey);

	    NavigableMap<String, IKvItem> map; 
	    String before = null;
	    
	    if(after == null)
	    {
	        map = partition;
	    }
	    else
	    {
	      map   = partition.tailMap(after, false);
	      before = map.isEmpty() ||  map.firstKey().equals(partition.firstKey()) ? null : map.firstKey();
	    }
	    
	    if(limit == null)
	      limit = 100;
	    
	    int available = map.entrySet().size();
	    
	    for(Entry<String, IKvItem> entry : map.entrySet())
	    {

	      available--;
	 
	      consumer.accept(new KvPartitionUser(entry.getKey(), entry.getValue().getJson()));
	      
	        if(--limit <= 0)
	          return new KvPagination(before, available==0 ? null : entry.getKey());
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

  @Override
  public int getTransactionItemsLimit()
  {
    return 25;
  }

}
