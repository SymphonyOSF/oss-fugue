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

package com.symphony.oss.fugue.aws.kv.table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CancellationReason;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.Delete;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTimeToLiveRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTimeToLiveResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.Tag;
import com.amazonaws.services.dynamodbv2.model.TagResourceRequest;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveDescription;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveSpecification;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amazonaws.services.dynamodbv2.model.Update;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.symphony.oss.commons.fault.CodingFault;
import com.symphony.oss.commons.fault.FaultAccumulator;
import com.symphony.oss.commons.hash.Hash;
import com.symphony.oss.fugue.Fugue;
import com.symphony.oss.fugue.aws.AwsTags;
import com.symphony.oss.fugue.kv.IKvItem;
import com.symphony.oss.fugue.kv.IKvPagination;
import com.symphony.oss.fugue.kv.IKvPartitionKeyProvider;
import com.symphony.oss.fugue.kv.IKvPartitionSortKeyProvider;
import com.symphony.oss.fugue.kv.KvCondition;
import com.symphony.oss.fugue.kv.KvPagination;
import com.symphony.oss.fugue.kv.KvPartitionUser;
import com.symphony.oss.fugue.kv.table.AbstractKvTable;
import com.symphony.oss.fugue.kv.table.IKvTableTransaction;
import com.symphony.oss.fugue.store.NoSuchObjectException;
import com.symphony.oss.fugue.store.TransactionFailedException;
import com.symphony.oss.fugue.trace.ITraceContext;
import com.symphony.oss.fugue.trace.NoOpTraceContext;

/**
 * DynamoDB implementation of IKvTable
 * 
 * @author Bruce Skingle
 *
 * @param <T> Concrete type for fluent methods.
 */
public abstract class AbstractDynamoDbKvTable<T extends AbstractDynamoDbKvTable<T>> extends AbstractKvTable<T>
{
  private static final Logger         log_                   = LoggerFactory.getLogger(AbstractDynamoDbKvTable.class);

  public static final String       ColumnNamePartitionKey = "pk";
  public static final String       ColumnNameSortKey      = "sk";
  public static final String       ColumnNameDocument     = "d";
  public static final String       ColumnNamePodId        = "p";
  public static final String       ColumnNamePayloadType  = "pt";
  public static final String       ColumnNameTTL          = "t";
  public static final String       ColumnNameCreatedDate  = "c";
  public static final String       ColumnNameAbsoluteHash = "h";

  protected static final int          MAX_RECORD_SIZE        = 400 * 1024;

  private static final String KEY_EXISTS = "An object with given partition and sort key already exists.";
  private static final String KEY_EXISTS_OR_OBJECT_CHANGED = "An object with given partition and sort key already exists, or the object to be updated has changed.";

  protected final String              region_;

  protected AmazonDynamoDB            amazonDynamoDB_;
  protected DynamoDB                  dynamoDB_;
  protected Table                     objectTable_;

  protected final String              objectTableName_;
  protected final int                 payloadLimit_;
  protected final boolean             validate_;
  protected final StreamSpecification streamSpecification_;
  
  protected AbstractDynamoDbKvTable(AbstractBuilder<?,?> builder)
  {
    super(builder);
    
    region_                 = builder.region_;
    payloadLimit_           = builder.payloadLimit_ != null ? builder.payloadLimit_ : MAX_RECORD_SIZE ;
    validate_               = builder.validate_;
    streamSpecification_    = builder.streamSpecification_;
  
    log_.info("Starting storage...");
    
    
    
    amazonDynamoDB_ = builder.amazonDynamoDBClientBuilder_.build();
    
    dynamoDB_               = new DynamoDB(amazonDynamoDB_);
    objectTableName_        = nameFactory_.getTableName("objects").toString();
    objectTable_            = dynamoDB_.getTable(objectTableName_);
    
        
    validate();
    
    log_.info("storage started.");
  }
  
  protected void validate()
  {
    if(validate_)
    {
      try(FaultAccumulator report = new FaultAccumulator())
      {
        try
        {
          objectTable_.describe();
        }
        catch(ResourceNotFoundException e)
        {
          report.error("Object table does not exist");
        }
      }
    }
  }

  /**
   * Return the name of this table.
   * 
   * @return the name of this table.
   */
  public String getTableName()
  {
    return objectTableName_;
  }

  @Override
  public String fetch(IKvPartitionSortKeyProvider partitionSortKey, ITraceContext trace) throws NoSuchObjectException
  {
    return doDynamoReadTask(() ->
    {
      GetItemSpec spec = new GetItemSpec().withPrimaryKey(ColumnNamePartitionKey, getPartitionKey(partitionSortKey), ColumnNameSortKey, partitionSortKey.getSortKey().toString());

      Item item = objectTable_.getItem(spec);
      
      if(item == null)
        throw new NoSuchObjectException("Item (" + getPartitionKey(partitionSortKey) + ", " + partitionSortKey.getSortKey() + ") not found.");
      
      String payloadString = item.getString(ColumnNameDocument);
      
      if(payloadString == null)
      {
        Hash absoluteHash = Hash.ofBase64String(item.getString(ColumnNameAbsoluteHash));
        
        payloadString = fetchFromSecondaryStorage(absoluteHash, trace);
      }
      
      return payloadString;
    });
  }

  @Override
  public String fetchFirst(IKvPartitionKeyProvider partitionKey, ITraceContext trace) throws NoSuchObjectException
  {
    return fetchOne(partitionKey, true, trace);
  }

  @Override
  public String fetchLast(IKvPartitionKeyProvider partitionKey, ITraceContext trace) throws NoSuchObjectException
  {
    return fetchOne(partitionKey, false, trace);
  }

  private String fetchOne(IKvPartitionKeyProvider partitionKey, boolean scanForwards, ITraceContext trace) throws NoSuchObjectException
  {
    return doDynamoReadTask(() ->
    {
      trace.trace("START_FETCH_ONE");
      QuerySpec spec = new QuerySpec()
        .withKeyConditionExpression(ColumnNamePartitionKey + " = :v_partition")
        .withMaxResultSize(1)
        .withValueMap(new ValueMap()
            .withString(":v_partition", getPartitionKey(partitionKey)))
        .withScanIndexForward(scanForwards)
        ;
    
      ItemCollection<QueryOutcome> items = objectTable_.query(spec);
      
      Iterator<Item> it = items.firstPage().iterator();
      
      if(it.hasNext())
      {
        Item item = it.next();
        
        String payloadString = item.getString(ColumnNameDocument);
            
        if(payloadString == null)
        {
          Hash absoluteHash = Hash.ofBase64String(item.getString(ColumnNameAbsoluteHash));
          
          payloadString = fetchFromSecondaryStorage(absoluteHash, trace);
        }
        
        trace.trace("DONE_FETCH_ONE");
        return payloadString;
      }
      
      throw new NoSuchObjectException(partitionKey + " not found");
    });
  }
  
  protected <CT> CT doDynamoQueryTask(Callable<CT> task)
  {
    try
    {
      return doDynamoReadTask(task);
    }
    catch (NoSuchObjectException e)
    {
      // This "can't happen"
      throw new CodingFault(e);
    }
  }

  protected <CT> CT doDynamoReadTask(Callable<CT> task) throws NoSuchObjectException
  {
    return doDynamoReadTask(task, NoOpTraceContext.INSTANCE);
  }

  protected <CT> CT doDynamoReadTask(Callable<CT> task, ITraceContext trace) throws NoSuchObjectException
  {
    return doDynamoTask(task, "read", trace);
  }

  @Override
  public Transaction createTransaction()
  {
    return new Transaction();
  }

  @Override
  public void store(Collection<IKvItem> kvItems, ITraceContext trace)
  {
    try
    {
//      store(null, kvItems, trace);
      
      Transaction transaction = new Transaction();
      
      transaction.store(null, kvItems);
      
      transaction.commit(trace);
    }
    catch (TransactionFailedException e)
    {
      throw new IllegalStateException(e);
    }
  }

//  @Override
//  public void store(@Nullable IKvPartitionSortKeyProvider partitionSortKeyProvider, Collection<IKvItem> kvItems, ITraceContext trace) throws ObjectExistsException
//  {
//    if(kvItems.isEmpty())
//      return;
//
//    Hash        absoluteHash = null;
//    List<TransactWriteItem> actions = new ArrayList<>(kvItems.size());
//    String    existingPartitionKey = partitionSortKeyProvider==null ? null : getPartitionKey(partitionSortKeyProvider);
//    String    existingSortKey = partitionSortKeyProvider==null ? null : partitionSortKeyProvider.getSortKey().asString();
//      
//    
//    Hash secondaryStoredHash = null;
//    
//    for(IKvItem kvItem : kvItems)
//    {
//      String partitionKey = getPartitionKey(kvItem);
//      String sortKey = kvItem.getSortKey().asString();
//      
//      UpdateOrPut updateOrPut = new UpdateOrPut(kvItem, partitionKey, sortKey, payloadLimit_);
//      
//      absoluteHash = kvItem.getAbsoluteHash();
//      
//      if(kvItem.isSaveToSecondaryStorage())
//      {
//        if(storeToSecondaryStorage(kvItem, updateOrPut.payloadNotStored_, trace))
//          secondaryStoredHash = kvItem.getAbsoluteHash();
//      }
//      
//      Put put = updateOrPut.createPut();
//      
//      // It's not strictly necessary to check both things for null but it stops the compiler complaining...
//      if(existingPartitionKey!=null && existingSortKey!=null && existingPartitionKey.equals(partitionKey) && existingSortKey.equals(sortKey))
//      {
//        put.withConditionExpression("attribute_not_exists(" + ColumnNamePartitionKey + ") and attribute_not_exists(" + ColumnNameSortKey + ")");
//      }
//      
//      actions.add(new TransactWriteItem().withPut(put));
//    }
//    
//    try
//    {
//      write(actions, absoluteHash.toStringBase64(), KEY_EXISTS, trace);
//    }
//    catch (NoSuchObjectException e)
//    {
//      log_.error("Failed to wite objects", e);
//      if(secondaryStoredHash != null)
//      {
//        try
//        {
//          deleteFromSecondaryStorage(secondaryStoredHash, trace);
//        }
//        catch(RuntimeException e2)
//        {
//          log_.error("Failed to delete secondary copy of " + secondaryStoredHash, e2);
//        }
//      }
//      throw new ObjectExistsException(KEY_EXISTS, e);
//    }
//  }
  
  @Override
  public void store(IKvItem kvItem, KvCondition kvCondition, ITraceContext trace)
  {
    Hash        absoluteHash = kvItem.getAbsoluteHash();
    List<TransactWriteItem> actions = new ArrayList<>(1);
    Hash secondaryStoredHash = null;
    
    String partitionKey = getPartitionKey(kvItem);
    String sortKey = kvItem.getSortKey().asString();
    
    UpdateOrPut updateOrPut = new UpdateOrPut(kvItem, partitionKey, sortKey, payloadLimit_);
    
    ;
    
    if(kvItem.isSaveToSecondaryStorage())
    {
      if(storeToSecondaryStorage(kvItem, updateOrPut.payloadNotStored_, trace))
        secondaryStoredHash = kvItem.getAbsoluteHash();
    }
    
    Condition condition = new Condition(
        "attribute_not_exists(" + kvCondition.getName() + ") or " +
        kvCondition.getName() + " " + kvCondition.getComparison().getSymbol() + " :v")
        .withString(":v", kvCondition.getValue());
    
    Put put = updateOrPut
        .createPut()
        .withConditionExpression(condition.expression_)
        .withExpressionAttributeValues(condition.attributeValues_);
    
    actions.add(new TransactWriteItem().withPut(put));
    
    try
    {
      trace.trace("ABOUT_TO_STORE_CONDITIONAL", kvItem);
      write(actions, absoluteHash.toStringBase64(), "Conditions not met.", trace);
      trace.trace("STORED_CONDITIONAL", kvItem);
    }
    catch (NoSuchObjectException e)
    {
      trace.trace("FAILED_TO_STORE_CONDITIONAL", kvItem);
      if(secondaryStoredHash != null)
      {
        try
        {
          deleteFromSecondaryStorage(secondaryStoredHash, trace);
        }
        catch(RuntimeException e2)
        {
          log_.error("Failed to delete secondary copy of " + secondaryStoredHash, e2);
        }
      }
    }
  }
  
  @Override
  public void storeEntitlementMapping(IKvItem kvItem, List<KvCondition> kvConditions, ITraceContext trace)
  {
    if(kvConditions.size() != 2)
      throw new IllegalArgumentException("Wrong Number of KvConditions, expected 2, got: " + kvConditions.size());
    
    Hash        absoluteHash = kvItem.getAbsoluteHash();
    List<TransactWriteItem> actions = new ArrayList<>(1);
    Hash secondaryStoredHash = null;
    
    String partitionKey = getPartitionKey(kvItem);
    String sortKey = kvItem.getSortKey().asString();
    
    UpdateOrPut updateOrPut = new UpdateOrPut(kvItem, partitionKey, sortKey, payloadLimit_);
    
    if(kvItem.isSaveToSecondaryStorage())
    {
      if(storeToSecondaryStorage(kvItem, updateOrPut.payloadNotStored_, trace))
        secondaryStoredHash = kvItem.getAbsoluteHash();
    }
    
    KvCondition effective = kvConditions.get(0);
    KvCondition entAction = kvConditions.get(1);
    
    String expression = "attribute_not_exists("+effective.getName()+") OR "+effective.getName()+" < :a"
                        +" and "
                        +"( "
                        +" ("
                        +"  attribute_not_exists("+entAction.getName()+") "
                        +"  AND "
                        +"  :b = ALLOW"
                        +")"
                        +" or "
                        +"("+entAction.getName()+" <> :b)"
                        +")";
    
    HashMap<String, AttributeValue> values = new HashMap<>();
    values.put(":a", new AttributeValue(effective.getValue()));
    values.put(":b", new AttributeValue(entAction.getValue()));
    
    Put put = updateOrPut
        .createPut()
        .withConditionExpression(expression)
        .withExpressionAttributeValues(values);
    
    actions.add(new TransactWriteItem().withPut(put));
    
    try
    {
      trace.trace("ABOUT_TO_STORE_CONDITIONAL", kvItem);
      write(actions, absoluteHash.toStringBase64(), "Conditions not met.", trace);
      trace.trace("STORED_CONDITIONAL", kvItem);
    }
    catch (NoSuchObjectException e)
    {
      trace.trace("FAILED_TO_STORE_CONDITIONAL", kvItem);
      if(secondaryStoredHash != null)
      {
        try
        {
          deleteFromSecondaryStorage(secondaryStoredHash, trace);
        }
        catch(RuntimeException e2)
        {
          log_.error("Failed to delete secondary copy of " + secondaryStoredHash, e2);
        }
      }
    }
  }

  class Condition
  {
    String                      expression_;
    Map<String, AttributeValue> attributeValues_ = new HashMap<>();
    
    Condition(String expression)
    {
      expression_ = expression;
    }
    
    Condition withString(String name, String value)
    {
      attributeValues_.put(name, new AttributeValue().withS(value));
      
      return this;
    }
  }
  
  abstract class AbstractDeleteConsumer extends AbstractItemConsumer
  {  
    List<PrimaryKey>            primaryKeysToDelete_ = new ArrayList<>(24);
    IKvPartitionSortKeyProvider absoluteHashPrefix_;
    
    void dynamoBatchWrite()
    {
      if(primaryKeysToDelete_.isEmpty())
        return;
      
      TableWriteItems tableWriteItems = new TableWriteItems(objectTable_.getTableName())
          .withPrimaryKeysToDelete(primaryKeysToDelete_.toArray(new PrimaryKey[primaryKeysToDelete_.size()]));
      
      BatchWriteItemOutcome outcome = dynamoDB_.batchWriteItem(tableWriteItems);
      int totalRequestItems = primaryKeysToDelete_.size();
      long  delay = 4;
      do
      {
          Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

          if (outcome.getUnprocessedItems().size() > 0)
          {
            int requestItems = 0;
            
            for(List<WriteRequest> ui : unprocessedItems.values())
            {
              requestItems += ui.size();
            }
      
            log_.info("Retry " + requestItems + " of " + totalRequestItems + " items after " + delay + "ms.");
            
            totalRequestItems = requestItems;
            
            try
            {
              Thread.sleep(delay);
              
              if(delay < 1000)
                delay *= 1.2;
            }
            catch (InterruptedException e)
            {
              log_.warn("Sleep interrupted", e);
            }
            
            outcome = dynamoDB_.batchWriteItemUnprocessed(unprocessedItems);
          }
      } while (outcome.getUnprocessedItems().size() > 0);
    }
  }
  
  class DeleteConsumer extends AbstractDeleteConsumer
  {
    
    public DeleteConsumer(IKvPartitionSortKeyProvider absoluteHashPrefix)
    {
      absoluteHashPrefix_ = absoluteHashPrefix;
    }

    @Override
    void consume(Item item, ITraceContext trace)
    {
      primaryKeysToDelete_.add(new PrimaryKey(
          new KeyAttribute(ColumnNamePartitionKey,  item.getString(ColumnNamePartitionKey)),
          new KeyAttribute(ColumnNameSortKey,       item.getString(ColumnNameSortKey))
          )
        );
      
      Hash absoluteHash = Hash.newInstance(item.getString(ColumnNameAbsoluteHash));
      
      primaryKeysToDelete_.add(new PrimaryKey(
          new KeyAttribute(ColumnNamePartitionKey,  getPartitionKey(absoluteHashPrefix_) + absoluteHash),
          new KeyAttribute(ColumnNameSortKey,       absoluteHashPrefix_.getSortKey().asString())
          )
        );
      
      try
      {
        deleteFromSecondaryStorage(absoluteHash, trace);
      }
      catch(RuntimeException e2)
      {
        log_.error("Failed to delete secondary copy of " + absoluteHash, e2);
      }

    }
  }
  
  class DeleteSystemObjectConsumer extends AbstractDeleteConsumer
  {

  @Override
  void consume(Item item, ITraceContext trace)
  {
    primaryKeysToDelete_.add(new PrimaryKey(
        new KeyAttribute(ColumnNamePartitionKey,  item.getString(ColumnNamePartitionKey)),
        new KeyAttribute(ColumnNameSortKey,       item.getString(ColumnNameSortKey))
        )
      );    
  }
}
  
  @Override
  public void delete(IKvPartitionSortKeyProvider partitionSortKeyProvider, 
      IKvPartitionKeyProvider versionPartitionKey, IKvPartitionSortKeyProvider absoluteHashPrefix, ITraceContext trace)
  {
    String    existingPartitionKey = getPartitionKey(partitionSortKeyProvider);
    String    existingSortKey = partitionSortKeyProvider.getSortKey().asString();
    
    String after = null;
    do
    {
      DeleteConsumer deleteConsumer = new DeleteConsumer(absoluteHashPrefix);
      
      after = doFetchPartitionObjects(versionPartitionKey, true, 12, after, null, null, deleteConsumer, trace).getAfter();
      
      deleteConsumer.dynamoBatchWrite();
    } while (after != null);
    
    final Map<String, AttributeValue> itemKey = new HashMap<>();
    
    itemKey.put(ColumnNamePartitionKey, new AttributeValue(existingPartitionKey));
    itemKey.put(ColumnNameSortKey, new AttributeValue(existingSortKey));
    
    amazonDynamoDB_.deleteItem(new DeleteItemRequest()
        .withTableName(objectTableName_)
        .withKey(itemKey)
        );
  }
  
  @Override
  public void deleteSystemPartitionObjects(IKvPartitionKeyProvider partitionKeyProvider, ITraceContext trace)
  {
    String after = null;
    
    do
    {
      DeleteSystemObjectConsumer deleteConsumer = new DeleteSystemObjectConsumer();
      
      after = doFetchPartitionObjects(partitionKeyProvider, true, 24, after, null, null, deleteConsumer, trace).getAfter();
      
      deleteConsumer.dynamoBatchWrite();
    } while (after != null);

  }
    
  @Override
  public void deleteRow(IKvPartitionSortKeyProvider partitionSortKeyProvider, ITraceContext trace)
  {
    String    existingPartitionKey = getPartitionKey(partitionSortKeyProvider);
    String    existingSortKey = partitionSortKeyProvider.getSortKey().asString();
    
    final Map<String, AttributeValue> itemKey = new HashMap<>();
    
    itemKey.put(ColumnNamePartitionKey, new AttributeValue(existingPartitionKey));
    itemKey.put(ColumnNameSortKey, new AttributeValue(existingSortKey));
    
    amazonDynamoDB_.deleteItem(new DeleteItemRequest()
        .withKey(itemKey)
        .withTableName(objectTable_.getTableName())
        );
  }
  
  public class Transaction implements IKvTableTransaction
  {
    String                  id_ = UUID.randomUUID().toString();
    List<TransactWriteItem> actions_ = new LinkedList<>();
    List<IKvItem>           secondaryStorageItemNotStored_ = new LinkedList<>();
    List<IKvItem>           secondaryStorageItemStored_ = new LinkedList<>();

    @Override
    public void commit(ITraceContext trace) throws TransactionFailedException
    {
      List<Hash> secondaryStoredHashes = new LinkedList<>();
      
      for(IKvItem kvItem : secondaryStorageItemNotStored_)
      {
        if(storeToSecondaryStorage(kvItem, true, trace))
          secondaryStoredHashes.add(kvItem.getAbsoluteHash());
      }
      
      for(IKvItem kvItem : secondaryStorageItemStored_)
      {
        if(storeToSecondaryStorage(kvItem, false, trace))
          secondaryStoredHashes.add(kvItem.getAbsoluteHash());
      }
      
      try
      {
        write(actions_, id_, KEY_EXISTS_OR_OBJECT_CHANGED, trace);
      }
      catch(AmazonDynamoDBException e)
      {
        int i = e.getErrorMessage().lastIndexOf(':');
        
        if(i != -1)
        {
          String s =  e.getErrorMessage().substring(i);
          
          if(s.startsWith(": Member must have length less than or equal to "))
          {
            throw new IllegalArgumentException("Transaction too large" + s);
          }
        }
        
        throw e;
      }
      catch (NoSuchObjectException e)
      {
        log_.error("Failed to wite objects", e);
         for(Hash secondaryStoredHash : secondaryStoredHashes)
        {
          try
          {
            deleteFromSecondaryStorage(secondaryStoredHash, trace);
          }
          catch(RuntimeException e2)
          {
            log_.error("Failed to delete secondary copy of " + secondaryStoredHash, e2);
          }
        }
        throw new TransactionFailedException(KEY_EXISTS_OR_OBJECT_CHANGED, e);
      }
    }

    @Override
    public void store(@Nullable IKvPartitionSortKeyProvider partitionSortKeyProvider, Collection<IKvItem> kvItems)
    {
      if(kvItems.isEmpty())
        return;

      //Hash        absoluteHash = null;
      String    existingPartitionKey = partitionSortKeyProvider==null ? null : getPartitionKey(partitionSortKeyProvider);
      String    existingSortKey = partitionSortKeyProvider==null ? null : partitionSortKeyProvider.getSortKey().asString();

      
      for(IKvItem kvItem : kvItems)
      {
        String partitionKey = getPartitionKey(kvItem);
        String sortKey = kvItem.getSortKey().asString();
        
        UpdateOrPut updateOrPut = new UpdateOrPut(kvItem, partitionKey, sortKey, payloadLimit_);
        
        if(kvItem.isSaveToSecondaryStorage())
        {
          if(updateOrPut.payloadNotStored_)
            secondaryStorageItemNotStored_.add(kvItem);
          else
            secondaryStorageItemStored_.add(kvItem);
        }
        
        Put put = updateOrPut.createPut();
        
        // It's not strictly necessary to check both things for null but it stops the compiler complaining...
        if(existingPartitionKey!=null && existingSortKey!=null && existingPartitionKey.equals(partitionKey) && existingSortKey.equals(sortKey))
        {
          put.withConditionExpression("attribute_not_exists(" + ColumnNamePartitionKey + ") and attribute_not_exists(" + ColumnNameSortKey + ")");
        }
        
        actions_.add(new TransactWriteItem().withPut(put));
      }
    }
    
    @Override
    public void update(IKvPartitionSortKeyProvider partitionSortKeyProvider, Hash absoluteHash, Set<IKvItem> kvItems)
    {
      String    existingPartitionKey = getPartitionKey(partitionSortKeyProvider);
      String    existingSortKey = partitionSortKeyProvider.getSortKey().asString();
      Condition condition = new Condition(ColumnNameAbsoluteHash + " = :ah").withString(":ah", absoluteHash.toStringBase64());
      
      for(IKvItem kvItem : kvItems)
      {
        String partitionKey = getPartitionKey(kvItem);
        String sortKey = kvItem.getSortKey().asString();
        
        UpdateOrPut updateOrPut = new UpdateOrPut(kvItem, partitionKey, sortKey, payloadLimit_);
        
        if(existingPartitionKey.equals(partitionKey) && existingSortKey.equals(sortKey))
        {
          actions_.add(new TransactWriteItem()
              .withUpdate(
                  updateOrPut.createUpdate(condition)
                  )
              );
          
          condition = null;
        }
        else
        {
          Put put = updateOrPut.createPut();
          
          if(existingPartitionKey.equals(partitionKey))
          {
            put.withConditionExpression("attribute_not_exists(" + ColumnNamePartitionKey + ") and attribute_not_exists(" + ColumnNameSortKey + ")");
          }
              
          actions_.add(new TransactWriteItem()
              .withPut(put)
              );
                  
        }
        
        if(kvItem.isSaveToSecondaryStorage())
        {
          if(updateOrPut.payloadNotStored_)
            secondaryStorageItemNotStored_.add(kvItem);
          else
            secondaryStorageItemStored_.add(kvItem);
        }
      }
      
      if(condition != null)
      {
        // The prev version has a different sort key

        final Map<String, AttributeValue> itemKey = new HashMap<>();
        
        itemKey.put(ColumnNamePartitionKey, new AttributeValue(existingPartitionKey));
        itemKey.put(ColumnNameSortKey, new AttributeValue(existingSortKey));
       
        Delete delete = new Delete()
            .withTableName(objectTable_.getTableName())
            .withConditionExpression(condition.expression_)
            .withExpressionAttributeValues(condition.attributeValues_)
            .withKey(itemKey)
            ;
        
        actions_.add(new TransactWriteItem().withDelete(delete));
      }
    }
  }
  
//  @Override
//  public void update(IKvPartitionSortKeyProvider partitionSortKeyProvider, Hash absoluteHash, Set<IKvItem> kvItems,
//      ITraceContext trace) throws NoSuchObjectException
//  {
//    List<TransactWriteItem> actions = new ArrayList<>(kvItems.size() + 2);
//    
//    String    existingPartitionKey = getPartitionKey(partitionSortKeyProvider);
//    String    existingSortKey = partitionSortKeyProvider.getSortKey().asString();
//    Condition condition = new Condition(ColumnNameAbsoluteHash + " = :ah").withString(":ah", absoluteHash.toStringBase64());
//    
//    Hash secondaryStoredHash = null;
//    
//    for(IKvItem kvItem : kvItems)
//    {
//      String partitionKey = getPartitionKey(kvItem);
//      String sortKey = kvItem.getSortKey().asString();
//      
//      UpdateOrPut updateOrPut = new UpdateOrPut(kvItem, partitionKey, sortKey, payloadLimit_);
//      
//      if(existingPartitionKey.equals(partitionKey) && existingSortKey.equals(sortKey))
//      {
//        actions.add(new TransactWriteItem()
//            .withUpdate(
//                updateOrPut.createUpdate(condition)
//                )
//            );
//        
//        condition = null;
//      }
//      else
//      {
//        Put put = updateOrPut.createPut();
//        
//        if(existingPartitionKey.equals(partitionKey))
//        {
//          put.withConditionExpression("attribute_not_exists(" + ColumnNamePartitionKey + ") and attribute_not_exists(" + ColumnNameSortKey + ")");
//        }
//            
//        actions.add(new TransactWriteItem()
//            .withPut(put)
//            );
//                
//      }
//      
//      if(kvItem.isSaveToSecondaryStorage())
//      {
//        if(storeToSecondaryStorage(kvItem, updateOrPut.payloadNotStored_, trace))
//          secondaryStoredHash = kvItem.getAbsoluteHash();
//      }
//    }
//    
//    String error = "Object to be updated (" + absoluteHash + ") has changed.";
//    
//    if(condition != null)
//    {
//      // The prev version has a different sort key
//
//      final Map<String, AttributeValue> itemKey = new HashMap<>();
//      
//      itemKey.put(ColumnNamePartitionKey, new AttributeValue(existingPartitionKey));
//      itemKey.put(ColumnNameSortKey, new AttributeValue(existingSortKey));
//     
//      Delete delete = new Delete()
//          .withTableName(objectTable_.getTableName())
//          .withConditionExpression(condition.expression_)
//          .withExpressionAttributeValues(condition.attributeValues_)
//          .withKey(itemKey)
//          ;
//      
//      actions.add(new TransactWriteItem().withDelete(delete));
//      
//      error = "Object to be updated (" + absoluteHash + ") has changed or an object already exists with the new sort key.";
//    }
//    
//    try
//    {
//      write(actions, absoluteHash.toStringBase64(), error, trace);
//    }
//    catch (NoSuchObjectException e)
//    {
//      if(secondaryStoredHash != null)
//      {
//        try
//        {
//          deleteFromSecondaryStorage(secondaryStoredHash, trace);
//        }
//        catch(RuntimeException e2)
//        {
//          log_.error("Failed to delete secondary copy of " + secondaryStoredHash, e2);
//        }
//      }
//      throw e;
//    }
//  }
  
  protected void write(Collection<TransactWriteItem> actions, String txnId, String errorMessage, ITraceContext trace) throws NoSuchObjectException
  {
    TransactWriteItemsRequest request = new TransactWriteItemsRequest()
        .withTransactItems(actions);
     
    doDynamoConditionalWriteTask(() -> 
    {
      int                           retryCnt = 0;
      long                          delay = 4;
      TransactionCanceledException  lastException = null;
      
      while(retryCnt++ < 11)
      {
        try
        {
          trace.trace("ABOUT_TO_STORE_TRANSACTIONAL", "OBJECT", txnId);
          amazonDynamoDB_.transactWriteItems(request);
          trace.trace("STORED_TRANSACTIONAL", "OBJECT", txnId);
          return null;
        }
        catch (TransactionCanceledException tce)
        {
          lastException = tce;
          
          for(CancellationReason reason : tce.getCancellationReasons())
          {
            switch(reason.getCode())
            {
              case "ConditionalCheckFailed":
                trace.trace("FAILED_FATAL_STORE_TRANSACTIONAL", "OBJECT", txnId);
                throw new NoSuchObjectException(errorMessage);
                
              case "None":
                // there is an entry for each item in the transaction, this means nothing and should be ignored.
                break;
                
              case "TransactionConflict":
                log_.info("Retry transaction after " + delay + "ms.");

                trace.trace("WAIT_RETRY_TO_STORE_TRANSACTIONAL", "OBJECT", txnId);
                try
                {
                  Thread.sleep(delay);
                  
                  if(delay < 1000)
                    delay *= 1.2;
                }
                catch (InterruptedException e)
                {
                  log_.warn("Sleep interrupted", e);
                }
                break;
                
              default:
                trace.trace("FAILED_TRANSIENT_STORE_TRANSACTIONAL", "OBJECT", txnId);
                throw new IllegalStateException("Transient failure to store object " + txnId, tce);
            }
          }
        }
        catch(RuntimeException e)
        {
          trace.trace("FAILED_TRANSIENT_STORE_TRANSACTIONAL", "OBJECT", txnId);
          throw e;
        }
      }
      trace.trace("FAILED_TRANSIENT_STORE_TRANSACTIONAL", "OBJECT", txnId);
      throw new IllegalStateException("Transient failure to update (after " + retryCnt + " retries) object " + txnId, lastException);
    }
    , trace);  
    
  }

  /**
   * Fetch the given item from secondary storage.
   * 
   * @param absoluteHash      Absolute hash of the required object.
   * @param trace             Trace context.
   * 
   * @return The required object.
   * 
   * @throws NoSuchObjectException If the required object does not exist.
   */
  protected abstract @Nonnull String fetchFromSecondaryStorage(Hash absoluteHash, ITraceContext trace) throws NoSuchObjectException;
  
  /**
   * Store the given item to secondary storage.
   * 
   * @param kvItem            An item to be stored.
   * @param payloadNotStored  The payload is too large to store in primary storage.
   * @param trace             A trace context.
   * @return true if the object was stored
   */
  protected abstract boolean storeToSecondaryStorage(IKvItem kvItem, boolean payloadNotStored, ITraceContext trace);

  /**
   * Delete the given object from secondary storage.
   * 
   * @param absoluteHash  Hash of the item to be deleted.
   * @param trace         A trace context.
   */
  protected abstract void deleteFromSecondaryStorage(Hash absoluteHash, ITraceContext trace);
  
//  protected void write(List<Put> items, ITraceContext trace)
//  {
//    doDynamoWriteTask(() -> 
//    {
//      dynamoBatchWrite(items);
//      trace.trace("WRITTEN-DYNAMODB");
//      
//      return null;
//     }
//    , trace);
//  }

  protected <TT> TT doDynamoTask(Callable<TT> task, String accessMode, ITraceContext trace) throws NoSuchObjectException
  {
    String message = "Failed to " + accessMode + " object";
    
    try
    {
      return task.call();
    }
    catch(ProvisionedThroughputExceededException e)
    {
      log_.warn(message + " - Provisioned Throughput Exceeded", e);
      trace.trace("FAILED-THROUGHPUT-DYNAMODB");
//      try
//      {
//        objectTableHelper_.scaleOutWrite();
//      }
//      catch(RuntimeException e2)
//      {
//        log_.error("Failed to scale out", e2);
//      }
      
      throw new IllegalStateException(message, e);
    } 
    catch (AmazonServiceException e)
    {
      trace.trace("FAILED-AWSEXCEPTION-DYNAMODB");
      log_.error(message, e);
      throw e;
    } 
    catch (NoSuchObjectException e)
    {
      throw e;
    } 
    catch (Exception e) // Callable made me do this...
    {
      trace.trace("FAILED-UNEXPECTED-DYNAMODB");
      log_.error("UNEXPECTED EXCEPTION", e);
      throw new IllegalStateException(message, e);
    }
  }
  
  protected void doDynamoWriteTask(Callable<Void> task, ITraceContext trace)
  {
    try
    {
      doDynamoTask(task, "write", trace);
    }
    catch (NoSuchObjectException e)
    {
      trace.trace("FAILED-UNEXPECTED-DYNAMODB");
      log_.error("UNEXPECTED EXCEPTION", e);
      throw new IllegalStateException("Failed to write object", e);
    }
  }
  
  protected void doDynamoConditionalWriteTask(Callable<Void> task, ITraceContext trace) throws NoSuchObjectException
  {
    try
    {
      doDynamoTask(task, "write", trace);
    }
    catch (NoSuchObjectException e)
    {
      throw e;
    }
  }

//  protected void dynamoBatchWrite(Collection<Put> itemsToPut)
//  {
//    TableWriteItems tableWriteItems = new TableWriteItems(objectTable_.getTableName())
//        .withItemsToPut(itemsToPut)
//        ;
//    
//    if(primaryKeysToDelete != null)
//      tableWriteItems = tableWriteItems.withPrimaryKeysToDelete(primaryKeysToDelete.toArray(new PrimaryKey[primaryKeysToDelete.size()]));
//    
//    Map<String, List<WriteRequest>> requestItems = new HashMap<>();
//    
//    new WriteRequest().withPutRequest(new PutRequest()
//    
//    List<WriteRequest> value;
//    requestItems.put(objectTable_.getTableName(), value);
//    BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest().withRequestItems(requestItems);
//    BatchWriteItemResult outcome = amazonDynamoDB_.batchWriteItem(batchWriteItemRequest);
//    
//    
//    int requestItems = itemsToPut.size();
//    long  delay = 4;
//    do
//    {
//        Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
//
//        if (outcome.getUnprocessedItems().size() > 0)
//        {
//          requestItems = 0;
//          
//          for(List<WriteRequest> ui : unprocessedItems.values())
//          {
//            requestItems += ui.size();
//          }
//    
//          log_.info("Retry " + requestItems + " of " + requestItems + " items after " + delay + "ms.");
//          try
//          {
//            Thread.sleep(delay);
//            
//            if(delay < 1000)
//              delay *= 1.2;
//          }
//          catch (InterruptedException e)
//          {
//            log_.warn("Sleep interrupted", e);
//          }
//          
//          outcome = dynamoDB_.batchWriteItemUnprocessed(unprocessedItems);
//        }
//    } while (outcome.getUnprocessedItems().size() > 0);
//  }
  
  class UpdateOrPut
  {
    private static final String INVALID_ATTR_KEY_LEN = "Additional attribute keys must be between 3 and 10 characters";
    
    Map<String, AttributeValue> key_              = new HashMap<>();
    ValueMap                    putItem_          = new ValueMap();
    Map<String, AttributeValue> updateItem_       = new HashMap<>();
    StringBuilder               updateExpression_ = new StringBuilder("SET ");
    int                         baseLength_;
    boolean                     payloadNotStored_;
    boolean                     first_            = true;
    
    UpdateOrPut(IKvItem kvItem, String partitionKey, String sortKey, int payloadLimit)
    {
      putItem_.withString(ColumnNamePartitionKey,  partitionKey);
      putItem_.withString(ColumnNameSortKey,       sortKey);
      
      key_.put(ColumnNamePartitionKey,  new AttributeValue(partitionKey));
      key_.put(ColumnNameSortKey,       new AttributeValue(sortKey));
      
      baseLength_ = ColumnNamePartitionKey.length() + partitionKey.length() + 
          ColumnNameSortKey.length() + sortKey.length();
      
      withHash(   ColumnNameAbsoluteHash, kvItem.getAbsoluteHash());
      
      if(kvItem.getPodId() != null)
        withNumber( ColumnNamePodId,        kvItem.getPodId().getValue());
      
      withString( ColumnNamePayloadType,  kvItem.getType());
      
      if(kvItem.getAdditionalAttributes() != null)
      {
        for(Entry<String, Object> entry : kvItem.getAdditionalAttributes().entrySet())
        {
          if(entry.getKey().length() < 3 || entry.getKey().length() >10)
            throw new IllegalArgumentException(INVALID_ATTR_KEY_LEN);
          
          if(entry.getValue() instanceof Number)
          {
            withNumber(entry.getKey(), (Number) entry.getValue());
          }
          else
          {
            withString(entry.getKey(), entry.getValue().toString());
          }
        }
      }
      
      if(kvItem.getPurgeDate() != null)
      {
        Long ttl = kvItem.getPurgeDate().toEpochMilli() / 1000;
        
        withNumber( ColumnNameTTL,          ttl);
      }
      
      int length = baseLength_ + ColumnNameDocument.length() + kvItem.getJson().length();
      
      if(length < payloadLimit)
      {
        payloadNotStored_ = false;
        withString(ColumnNameDocument, kvItem.getJson());
      }
      else
      {
        payloadNotStored_ = true;
      }
    }
    
    Put createPut()
    {
      return new Put()
        .withTableName(objectTable_.getTableName())
        .withItem(ItemUtils.fromSimpleMap(putItem_));
    }
    
    Update createUpdate(Condition condition)
    {
      updateItem_.putAll(condition.attributeValues_);
      
      return new Update()
        .withTableName(objectTable_.getTableName())
        .withConditionExpression(condition.expression_)
        .withExpressionAttributeValues(updateItem_)
        .withKey(key_)
        .withUpdateExpression(updateExpression_.toString())
      ;
    }

    private void withNumber(String name, Number value)
    {
      separator();
      updateExpression_.append(name + " = :" + name);
      
      if(value == null)
      {
        putItem_.withNull(name);
        updateItem_.put(":" + name, new AttributeValue().withNULL(true));
        baseLength_ += name.length();
      }
      else
      {
        putItem_.withNumber(name, value);
        updateItem_.put(":" + name, new AttributeValue().withN(value.toString()));
        baseLength_ += name.length() + value.toString().length();
      }
    }

    private void separator()
    {
      if(first_)
        first_ = false;
      else
        updateExpression_.append(", ");
      
    }

    private void withHash(String name, Hash value)
    {
      withString(name, value == null ? null : value.toStringBase64());
    }

    private void withString(String name, String value)
    {
      separator();
      updateExpression_.append(name + " = :" + name);
    
      if(value == null)
      {
        baseLength_ += name.length();
        putItem_.withNull(name);
        updateItem_.put(":" + name, new AttributeValue().withNULL(true));
      }
      else
      {
        baseLength_ += name.length() + value.length();
        putItem_.withString(name, value);
        updateItem_.put(":" + name, new AttributeValue().withS(value));
      }
    }

//    private void withJSON(String name, String value)
//    {
//      updateExpression_.append(name + " = :" + name);
//      
//      if(value == null)
//      {
//        baseLength_ += name.length();
//        putItem_.withNull(name);
//        updateItem_.put(":" + name, new AttributeValue().withNULL(true));
//      }
//      else
//      {
//        baseLength_ += name.length() + value.length();
//        putItem_.withJSON(name, value);
//        updateItem_.put(":" + name, new AttributeValue().with(value));
//        updateItem_.withJSON(":" + name, value);
//      }
//    }
  }
  
//  protected boolean createPut(IKvItem kvItem, String partitionKey, String sortKey, int payloadLimit, List<Put> items, Condition condition)
//  {
//    ValueMap putItem = new ValueMap();
//    ValueMap updateItem = new ValueMap();
//    StringBuilder updateExpression = new StringBuilder();
//    
//    putItem.withString(ColumnNamePartitionKey,  partitionKey);
//    putItem.withString(ColumnNameSortKey,       sortKey);
//    
//    int baseLength = ColumnNamePartitionKey.length() + partitionKey.length() + 
//        ColumnNameSortKey.length() + sortKey.length();
//    
//    if(kvItem.getAbsoluteHash() == null)
//    {
//      updateExpression.append(ColumnNameAbsoluteHash + " = null");
//    }
//    else
//    {
//      String ah = kvItem.getAbsoluteHash().toStringBase64();
//      
//      baseLength += ColumnNameAbsoluteHash.length() + ah.length();
//      putItem.withString(ColumnNameAbsoluteHash, ah);
//    }
//    
//    if(kvItem.getPodId() != null)
//    {
//      baseLength += ColumnNamePodId.length() + kvItem.getPodId().toString().length();
//      
//      putItem.withNumber(ColumnNamePodId, kvItem.getPodId().getValue());
//    }
//
//    if(kvItem.getType() != null)
//    {
//      baseLength += ColumnNamePayloadType.length() + kvItem.getType().length();
//      
//      putItem.withString(ColumnNamePayloadType, kvItem.getType());
//    }
//    
//    if(kvItem.getPurgeDate() != null)
//    {
//      long ttl = kvItem.getPurgeDate().toEpochMilli() / 1000;
//      
//      baseLength += ColumnNameTTL.length() + String.valueOf(ttl).length();
//      
//      putItem.withNumber(ColumnNameTTL, ttl);
//    }
//    
//    int length = baseLength + ColumnNameDocument.length() + kvItem.getJson().length();
//    
//    if(length < payloadLimit)
//    {
//      putItem.withJSON(ColumnNameDocument, kvItem.getJson());
//    }
//    
//    
//    
//    if(condition == null)
//    {
//      
//      Put put = new Put()
//          .withTableName(objectTable_.getTableName())
//          .withItem(ItemUtils.fromSimpleMap(putItem));
//    }
//    else
//    {
//      Update update = new Update()
//          .withTableName(objectTable_.getTableName())
//          .with
//          ;
//      put
//        .withConditionExpression(condition.expression_)
//        .withExpressionAttributeValues(condition.attributeValues_)
//        ;
//    }
//    
//    items.add(put);
//    
//    return length >= payloadLimit;
//  }
  

//  protected Put createPut2(IKvItem kvItem, String partitionKey, String sortKey, int payloadLimit)
//  {
//    HashMap<String, AttributeValue> item = new HashMap<>();
//    
//    item.put(ColumnNamePartitionKey,  new AttributeValue(partitionKey));
//    item.put(ColumnNameSortKey,       new AttributeValue(sortKey));
//    
//    int baseLength = ColumnNamePartitionKey.length() + partitionKey.length() + 
//        ColumnNameSortKey.length() + sortKey.length();
//    
//    if(kvItem.getAbsoluteHash() != null)
//    {
//      String ah = kvItem.getAbsoluteHash().toStringBase64();
//      
//      baseLength += ColumnNameAbsoluteHash.length() + ah.length();
//      item.put(ColumnNameAbsoluteHash, new AttributeValue(ah));
//    }
//    
//    if(kvItem.getPodId() != null)
//    {
//      baseLength += ColumnNamePodId.length() + kvItem.getPodId().toString().length();
//      
//      item.put(ColumnNamePodId, new AttributeValue().withN(kvItem.getPodId().getValue()));
//    }
//
//    if(kvItem.getType() != null)
//    {
//      baseLength += ColumnNamePayloadType.length() + kvItem.getType().length();
//      
//      item.put(ColumnNamePayloadType, new AttributeValue(kvItem.getType()));
//    }
//    
//    if(kvItem.getPurgeDate() != null)
//    {
//      String ttl = String.valueOf(kvItem.getPurgeDate().toEpochMilli() / 1000);
//      
//      baseLength += ColumnNameTTL.length() + ttl.length();
//      
//      item.put(ColumnNameTTL, new AttributeValue().withN(ttl));
//    }
//    
//    int length = baseLength + ColumnNameDocument.length() + kvItem.getJson().length();
//    
//    if(length < payloadLimit)
//    {
//      
//      item.put(ColumnNameDocument, new AttributeValue().withM(
//          valueConformer.transform(Jackson.fromJsonString(kvItem.getJson(), Object.class))));
//      return false;
//    }
//    else
//    {
//      return true;
//    }
//  }

//  protected boolean createPutItem(List<Item> items, IKvItem kvItem, String partitionKey, String sortKey, int payloadLimit)
//  {
//    Item item = new Item()
//        .withPrimaryKey(ColumnNamePartitionKey, 
//            partitionKey, 
//            ColumnNameSortKey, sortKey);
//    
//    int baseLength = ColumnNamePartitionKey.length() + partitionKey.length() + 
//        ColumnNameSortKey.length() + sortKey.length();
//    
//    if(kvItem.getAbsoluteHash() != null)
//    {
//      String ah = kvItem.getAbsoluteHash().toStringBase64();
//      
//      baseLength += ColumnNameAbsoluteHash.length() + ah.length();
//      
//      item.withString(ColumnNameAbsoluteHash, ah);
//    }
//    
//    if(kvItem.getPodId() != null)
//    {
//      baseLength += ColumnNamePodId.length() + kvItem.getPodId().toString().length();
//      
//      item.withInt(ColumnNamePodId, kvItem.getPodId().getValue());
//    }
//
//    if(kvItem.getType() != null)
//    {
//      baseLength += ColumnNamePayloadType.length() + kvItem.getType().length();
//      
//      item.withString(ColumnNamePayloadType, kvItem.getType());
//    }
//    
//    if(kvItem.getPurgeDate() != null)
//    {
//      long ttl = kvItem.getPurgeDate().toEpochMilli() / 1000;
//      
//      baseLength += ColumnNameTTL.length() + String.valueOf(ttl).length();
//      
//      item.withLong(ColumnNameTTL,       ttl);
//    }
//    
//    items.add(item);
//    
//    int length = baseLength + ColumnNameDocument.length() + kvItem.getJson().length();
//    
//    if(length < payloadLimit)
//    {
//      item.withJSON(ColumnNameDocument, kvItem.getJson());
//      return false;
//    }
//    else
//    {
//      return true;
//    }
//  }

  private String getPartitionKey(IKvPartitionKeyProvider kvItem)
  {
    return serviceId_ + Separator + kvItem.getPartitionKey();
  }

  @Override
  public void start()
  {
  }

  @Override
  public void stop()
  {
    if(amazonDynamoDB_ != null)
      amazonDynamoDB_.shutdown();
  }

//  private DynamoDbTableAdmin createTableAdmin()
//  {
//    return new DynamoDbTableAdmin(nameFactory_, objectTable_, getAmazonDynamoDB(), stsManager_)
//    {
//    
//      @Override
//      protected CreateTableRequest createCreateTableRequest()
//      {
//        return new CreateTableRequest()
//
//            .withTableName(objectTable_.getTableName())
//            .withAttributeDefinitions(
//                new AttributeDefinition(ColumnNameHashKey, ScalarAttributeType.S),
//                new AttributeDefinition(ColumnNameSortKey, ScalarAttributeType.S)
//                )
//            .withKeySchema(new KeySchemaElement(ColumnNameHashKey, KeyType.HASH), new KeySchemaElement(ColumnNameSortKey, KeyType.RANGE))
//            ;
//      }
//    }
//    .withTtlColumnName(ColumnNameTTL);
//  }
  
  @Override
  public void createTable(boolean dryRun)
  {
    HashMap<String,String> tagMap = new HashMap<>(nameFactory_.getTags());
    
    tagMap.put(Fugue.TAG_FUGUE_SERVICE, serviceId_);
    tagMap.put(Fugue.TAG_FUGUE_ITEM, objectTableName_);
    
    List<Tag> tags = new AwsTags(tagMap).getDynamoTags();
    
    String tableArn;
    
    try
    {
      TableDescription tableInfo = amazonDynamoDB_.describeTable(objectTableName_).getTable();

      tableArn = tableInfo.getTableArn();

      log_.info("Table \"" + objectTableName_ + "\" already exists as " + tableArn);
      
      configureStream(streamSpecification_, tableInfo);
    }
    catch (ResourceNotFoundException e)
    {
      // Table does not exist, create it
      
      if(dryRun)
      {
        log_.info("Table \"" + objectTableName_ + "\" does not exist and would be created");
        return;
      }
      else
      {
        try
        {
          CreateTableRequest    request;
          CreateTableResult     result;
          
          request = new CreateTableRequest()
              .withTableName(objectTable_.getTableName())
              .withAttributeDefinitions(
                  new AttributeDefinition(ColumnNamePartitionKey, ScalarAttributeType.S),
                  new AttributeDefinition(ColumnNameSortKey, ScalarAttributeType.S)
                  )
              .withKeySchema(new KeySchemaElement(ColumnNamePartitionKey, KeyType.HASH), new KeySchemaElement(ColumnNameSortKey, KeyType.RANGE))
              .withBillingMode(BillingMode.PAY_PER_REQUEST)
              .withStreamSpecification(streamSpecification_)
              ;
          
          result = amazonDynamoDB_.createTable(request);
          tableArn = result.getTableDescription().getTableArn();
          
          log_.info("Table \"" + objectTableName_ + "\" created as " + tableArn);
        }
        catch (RuntimeException e2)
        {
          log_.error("Failed to create tables", e2);
          throw e2;
        }
              
        try
        {
          objectTable_.waitForActive();
        }
        catch (InterruptedException e2)
        {
          throw new IllegalStateException(e2);
        }
      }
    }
    
    
//    configureAutoScale();
    
    try
    {
      DescribeTimeToLiveRequest describeTimeToLiveRequest = new DescribeTimeToLiveRequest().withTableName(objectTableName_);
      
      DescribeTimeToLiveResult ttlDescResult = amazonDynamoDB_.describeTimeToLive(describeTimeToLiveRequest);
      
      TimeToLiveDescription ttlDesc = ttlDescResult.getTimeToLiveDescription();
      
      if("ENABLED".equals(ttlDesc.getTimeToLiveStatus()))
      {
        log_.info("Table \"" + objectTableName_ + "\" already has TTL enabled.");
      }
      else
      {
        if(dryRun)
        {
          log_.info("Table \"" + objectTableName_ + "\" does not have TTL set and it would be set for column " + ColumnNameTTL);
        }
        else
        {
          //table created now enabling TTL
          UpdateTimeToLiveRequest req = new UpdateTimeToLiveRequest();
          req.setTableName(objectTableName_);
           
          TimeToLiveSpecification ttlSpec = new TimeToLiveSpecification();
          ttlSpec.setAttributeName(ColumnNameTTL);
          ttlSpec.setEnabled(true);
           
          req.withTimeToLiveSpecification(ttlSpec);
           
          UpdateTimeToLiveResult result2 = amazonDynamoDB_.updateTimeToLive(req);
          log_.info("Table \"" + objectTableName_ + "\" TTL updated " + result2);
        }
      }
    }
    catch (RuntimeException e)
    {
      log_.info("Failed to update TTL for table \"" + objectTableName_ + "\"", e);
      throw e;
    }
    
    try
    {
      amazonDynamoDB_.tagResource(new TagResourceRequest()
          .withResourceArn(tableArn)
          .withTags(tags)
          );
      log_.info("Table \"" + objectTableName_ + "\" tagged");
    }
    catch (RuntimeException e)
    {
      log_.error("Failed to add tags", e);
      throw e;
    }
    
    try
    {
      objectTable_.waitForActive();
    }
    catch (InterruptedException e)
    {
      throw new IllegalStateException(e);
    }
  }

//  private void configureAutoScale()
//  {
//    boolean updateTable = false;
//    UpdateTableRequest  updateRequest = new UpdateTableRequest()
//        .withTableName(objectTableName_);
//    
//    TableDescription tableInfo = amazonDynamoDB_.describeTable(objectTableName_).getTable();
//    
//    if(tableInfo.getBillingModeSummary() != null && BillingMode.PAY_PER_REQUEST.toString().equals(tableInfo.getBillingModeSummary().getBillingMode()))
//    {
//      log_.info("Table is set to on-demand - no change made.");
//    }
//    else
//    {
//      log_.info("Updating table to on-demand mode");
//      updateRequest.withBillingMode(BillingMode.PAY_PER_REQUEST);
//      updateTable=true;
//    }
//    
//    if(updateTable)
//    {
//      try
//      {
//        amazonDynamoDB_.updateTable(updateRequest);
//      }
//      catch(AmazonDynamoDBException e)
//      {
//        log_.error("Unable to update table throughput.", e);
//      }
//    }
//  }
  
  private void configureStream(StreamSpecification streamSpecification, TableDescription tableInfo)
  {
    String streamArn = tableInfo.getLatestStreamArn();
    
    if(streamSpecification == null || !streamSpecification.isStreamEnabled())
    {
      if(streamArn != null || (tableInfo.getStreamSpecification() != null && tableInfo.getStreamSpecification().isStreamEnabled()))
      {
        log_.info("Table has streams enabled, disabling....");
        streamSpecification = new StreamSpecification().withStreamEnabled(false);
      }
      else
      {
        log_.info("Table does not have streams enabled, nothing to do here.");
        return;
      }
    }
    else
    {
      if(streamArn == null || !tableInfo.getStreamSpecification().isStreamEnabled())
      {
        log_.info("Enabling streams for table....");
      }
      else if(!tableInfo.getStreamSpecification().getStreamViewType().equals(streamSpecification.getStreamViewType())
          && tableInfo.getStreamSpecification().getStreamEnabled()
          && streamSpecification.getStreamEnabled())
      {
        log_.info("Changing stream view type for table, ....");
        
        StreamSpecification disabled = new StreamSpecification()
            //.withStreamViewType(tableInfo.getStreamSpecification().getStreamViewType())
            .withStreamEnabled(false);
        
        UpdateTableRequest  updateTableRequest = new UpdateTableRequest()
            .withTableName(objectTable_.getTableName())
            .withStreamSpecification(disabled)
            ;
        
        amazonDynamoDB_.updateTable(updateTableRequest);
        
        log_.info("Waiting for table to be active...");
        try
        {
          objectTable_.waitForActive();
        }
        catch (InterruptedException e2)
        {
          throw new IllegalStateException(e2);
        }
        log_.info("Waiting for table to be active...DONE");
      }
      else
      {
        log_.info("Table has streams enabled, nothing to do here.");
        return;
      }
    }
    
    
    UpdateTableRequest  updateTableRequest = new UpdateTableRequest()
        .withTableName(objectTable_.getTableName())
        .withStreamSpecification(streamSpecification)
        ;
    
    amazonDynamoDB_.updateTable(updateTableRequest);
    
    log_.info("Stream settings updated.");
  }

  @Override
  public void deleteTable(boolean dryRun)
  {
    try
    {
      TableDescription tableInfo = amazonDynamoDB_.describeTable(objectTableName_).getTable();

      String tableArn = tableInfo.getTableArn();
      
      if(dryRun)
      {
        log_.info("Table \"" + objectTableName_ + "\" with arn " + tableArn + " would be deleted (dry run).");
      }
      else
      {
        log_.info("Deleting table \"" + objectTableName_ + "\" with arn " + tableArn + "...");

        amazonDynamoDB_.deleteTable(new DeleteTableRequest()
            .withTableName(objectTableName_));
      }
    }
    catch (ResourceNotFoundException e)
    {
      log_.info("Table \"" + objectTableName_ + "\" Does not exist.");
    }
  }

  @Override
  public IKvPagination fetchPartitionObjects(IKvPartitionKeyProvider partitionKey, boolean scanForwards, Integer limit, 
      @Nullable String after,
      @Nullable String sortKeyPrefix,
      @Nullable Map<String, Object> filterAttributes,
      Consumer<String> consumer, ITraceContext trace)
  {
    return doFetchPartitionObjects(partitionKey, scanForwards, limit, after, sortKeyPrefix, filterAttributes, new PartitionConsumer(consumer), trace);
  }

  private IKvPagination doFetchPartitionObjects(IKvPartitionKeyProvider partitionKey, boolean scanForwards, Integer limit, 
      @Nullable String after,
      @Nullable String sortKeyPrefix,
      @Nullable Map<String, Object> filterAttributes,
      AbstractItemConsumer consumer, ITraceContext trace)
  {
    return doDynamoQueryTask(() ->
    {
      ValueMap valueMap = new ValueMap()
          .withString(":v_partition", getPartitionKey(partitionKey))
          ;
      
      String keyConditionExpression = ColumnNamePartitionKey + " = :v_partition";
      
      if(sortKeyPrefix != null)
      {
        keyConditionExpression += " and begins_with(" + ColumnNameSortKey + ", :v_sortKeyPrefix)";
        valueMap.put(":v_sortKeyPrefix", sortKeyPrefix);
      }
      
      StringBuilder filter = null;
      
      if(filterAttributes != null)
      {
        for(Entry<String, Object> entry : filterAttributes.entrySet())
        {
          if(filter == null)
            filter = new StringBuilder();
          else
            filter.append(" and ");
          
          filter.append(entry.getKey());
          filter.append(" = :f_" );
          filter.append(entry.getKey());
          valueMap.put(":f_" + entry.getKey(), entry.getValue());
        }
      }
      
      QuerySpec spec = new QuerySpec()
          .withKeyConditionExpression(keyConditionExpression)
          .withValueMap(valueMap)
          .withScanIndexForward(scanForwards)
          ;
      
      if(filter != null)
      {
        spec.withFilterExpression(filter.toString());
      }
      
      if(limit != null)
      {
        spec.withMaxResultSize(limit);
      }
      
      if(after != null && after.length()>0)
      {
        spec.withExclusiveStartKey(
            new KeyAttribute(ColumnNamePartitionKey, getPartitionKey(partitionKey)),
            new KeyAttribute(ColumnNameSortKey,  after)
            );
      }
    
      Map<String, AttributeValue> lastEvaluatedKey = null;
      ItemCollection<QueryOutcome> items = objectTable_.query(spec);
      String before = null;
      for(Page<Item, QueryOutcome> page : items.pages())
      {
        Iterator<Item> it = page.iterator();
        
        while(it.hasNext())
        {
          Item item = it.next();
          
          consumer.consume(item, trace);
          
          if(before == null && after != null)
          {
            before = item.getString(ColumnNameSortKey);
          }
        }
      }
      
      if(before == null && after != null)
      {
        before = "";
      }
      
      lastEvaluatedKey = items.getLastLowLevelResult().getQueryResult().getLastEvaluatedKey();
      
      if(lastEvaluatedKey != null)
      {
        AttributeValue sequenceKeyAttr = lastEvaluatedKey.get(ColumnNameSortKey);
        
        return new KvPagination(before, sequenceKeyAttr.getS());
      }
      
      return new KvPagination(before, null);
    });
  }
  
  @Override
  public IKvPagination fetchPartitionUsers(IKvPartitionKeyProvider partitionKey, Integer limit, 
      @Nullable String after,
      Consumer<KvPartitionUser> consumer, ITraceContext trace)
  {
    return doFetchPartitionUsers(partitionKey, limit, after, new PartitionUserConsumer(consumer), trace);
  }

  private IKvPagination doFetchPartitionUsers(IKvPartitionKeyProvider partitionKey, Integer limit, 
      @Nullable String after,
      AbstractItemConsumer consumer, ITraceContext trace)
  {
	    ValueMap valueMap = new ValueMap()
	            .withString(":v_partition", getPartitionKey(partitionKey))
	            ;
	        
	        String keyConditionExpression = ColumnNamePartitionKey + " = :v_partition";
	        
	        QuerySpec spec = new QuerySpec()
	            .withKeyConditionExpression(keyConditionExpression)
	            .withValueMap(valueMap);
	            
	        
	        if(limit != null)
	        {
	          spec.withMaxResultSize(limit);
	        }
	        
	        if(after != null && after.length()>0)
	        {
	          spec.withExclusiveStartKey(
	              new KeyAttribute(ColumnNamePartitionKey, getPartitionKey(partitionKey)),
	              new KeyAttribute(ColumnNameSortKey,      after)
	              );
	        }
	      
	        Map<String, AttributeValue> lastEvaluatedKey = null;
	        ItemCollection<QueryOutcome> items = objectTable_.query(spec);
	        String before = null;
	        for(Page<Item, QueryOutcome> page : items.pages())
	        {
	          Iterator<Item> it = page.iterator();
	          
	          while(it.hasNext())
	          {
	            Item item = it.next();
	            
	            consumer.consume(item, trace);
	            
	            if(before == null && after != null)
	            {
	              before = item.getString(ColumnNameSortKey);
	            }
	          }
	        }
	        
	        if(before == null && after != null)
	        {
	          before = "";
	        }
	        
	        lastEvaluatedKey = items.getLastLowLevelResult().getQueryResult().getLastEvaluatedKey();
	        
	        if(lastEvaluatedKey != null)
	        {
	          AttributeValue sequenceKeyAttr = lastEvaluatedKey.get(ColumnNameSortKey);
	          
	          return new KvPagination(before, sequenceKeyAttr.getS());
	        }
	        
	        return new KvPagination(before, null);
   };

  abstract class AbstractItemConsumer
  {
    abstract void consume(Item item, ITraceContext trace);
  }
  
  class PartitionConsumer extends AbstractItemConsumer
  {
    Consumer<String> consumer_;
    
    PartitionConsumer(Consumer<String> consumer)
    {
      consumer_ = consumer;
    }

    @Override
    void consume(Item item, ITraceContext trace)
    {
      String payloadString = item.getString(ColumnNameDocument);
      
      if(payloadString == null)
      {
        String hashString = item.getString(ColumnNameAbsoluteHash);
        Hash absoluteHash = Hash.newInstance(hashString);
        
        try
        {
          payloadString = fetchFromSecondaryStorage(absoluteHash, trace);
        }
        catch (NoSuchObjectException e)
        {
          throw new IllegalStateException("Unable to read known object from S3", e);
        }
      }
      
      consumer_.accept(payloadString);
    }
  }
  
  class PartitionUserConsumer extends AbstractItemConsumer
  {
    Consumer<KvPartitionUser> consumer_;
    
    PartitionUserConsumer(Consumer<KvPartitionUser> consumer)
    {
      consumer_ = consumer;
    }
    @Override
    void consume(Item item, ITraceContext trace)
    {
      String payloadString = item.getString(ColumnNameDocument);
      
      if(payloadString == null)
      {
        String hashString = item.getString(ColumnNameAbsoluteHash);
        Hash absoluteHash = Hash.newInstance(hashString);
        
        try
        {
          payloadString = fetchFromSecondaryStorage(absoluteHash, trace);
        }
        catch (NoSuchObjectException e)

        {
          throw new IllegalStateException("Unable to read known object from S3", e);

        }
      }
      
      String sortKey = item.getString(ColumnNameSortKey);
      consumer_.accept(new KvPartitionUser(sortKey, payloadString));

    }
  }
  

  protected static abstract class AbstractBuilder<T extends AbstractBuilder<T,B>, B extends AbstractDynamoDbKvTable<B>> extends AbstractKvTable.AbstractBuilder<T,B>
  {
    protected final AmazonDynamoDBClientBuilder amazonDynamoDBClientBuilder_;

    protected String              region_;
    protected Integer             payloadLimit_           = MAX_RECORD_SIZE;
    protected boolean             validate_               = true;
    protected boolean             enableSecondaryStorage_ = false;

    protected StreamSpecification streamSpecification_    = new StreamSpecification().withStreamEnabled(false);
    
    protected AbstractBuilder(Class<T> type)
    {
      super(type);
      
      amazonDynamoDBClientBuilder_ = AmazonDynamoDBClientBuilder.standard();
    }
    
    @Override
    public void validate(FaultAccumulator faultAccumulator)
    {
      super.validate(faultAccumulator);
      
      faultAccumulator.checkNotNull(region_,      "region");
    }

    public T withValidate(boolean validate)
    {
      validate_ = validate;
      
      return self();
    }
    
    public T withStreamSpecification(StreamSpecification streamSpecification)
    {
      streamSpecification_ = streamSpecification;
      
      return self();
    }

    public T withEnableSecondaryStorage(boolean enableSecondaryStorage)
    {
      enableSecondaryStorage_ = enableSecondaryStorage;
      
      return self();
    }

    public T withRegion(String region)
    {
      region_ = region;
      
      return self();
    }

    public T withPayloadLimit(Integer payloadLimit)
    {
      payloadLimit_ = payloadLimit == null ? MAX_RECORD_SIZE : Math.min(payloadLimit, MAX_RECORD_SIZE);

      return self();
    }

    public T withCredentials(AWSCredentialsProvider credentials)
    {
      amazonDynamoDBClientBuilder_.withCredentials(credentials);
      
      return self();
    }
  }
 }

