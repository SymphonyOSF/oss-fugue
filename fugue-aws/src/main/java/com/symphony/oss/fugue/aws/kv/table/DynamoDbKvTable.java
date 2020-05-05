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

import javax.annotation.Nonnull;

import com.symphony.oss.commons.hash.Hash;
import com.symphony.oss.fugue.kv.IKvItem;
import com.symphony.oss.fugue.store.NoSuchObjectException;
import com.symphony.oss.fugue.trace.ITraceContext;

/**
 * DynamoDB implementation of IKvTable.
 * 
 * @author Bruce Skingle
 *
 */
public class DynamoDbKvTable extends AbstractDynamoDbKvTable<DynamoDbKvTable>
{ 
  protected DynamoDbKvTable(AbstractDynamoDbKvTable.AbstractBuilder<?,?> builder)
  {
    super(builder);
  }
  
  @Override
  protected @Nonnull String fetchFromSecondaryStorage(Hash absoluteHash, ITraceContext trace) throws NoSuchObjectException
  {
    throw new NoSuchObjectException("This table does not support secondary storage.");
  }
  
  @Override
  protected boolean storeToSecondaryStorage(IKvItem kvItem, boolean payloadNotStored, ITraceContext trace)
  {
    if(payloadNotStored)
      throw new IllegalArgumentException("This table does not support secondary storage and the payload is too large to store in primary storage.");
    
    return false;
  }
  
  @Override
  protected void deleteFromSecondaryStorage(Hash absoluteHash, ITraceContext trace)
  {
  }

  /**
   * Builder for DynamoDbKvTable.
   * 
   * @author Bruce Skingle
   *
   */
  public static class Builder extends AbstractDynamoDbKvTable.AbstractBuilder<Builder, DynamoDbKvTable>
  {
    /**
     * Constructor.
     */
    public Builder()
    {
      super(Builder.class); 
    }

    @Override
    protected DynamoDbKvTable construct()
    {
      return new DynamoDbKvTable(this);
    }

    @Override
    public Builder withEnableSecondaryStorage(boolean enableSecondaryStorage)
    {
      throw new IllegalArgumentException("Secondary storage is not supported by this implementation. Try S3DynamoDbKvTable");
    }
  }
}
