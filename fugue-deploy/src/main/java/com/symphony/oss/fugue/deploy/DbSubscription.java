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

package com.symphony.oss.fugue.deploy;

import com.symphony.oss.commons.dom.json.JsonObject;
import com.symphony.oss.fugue.naming.INameFactory;

public abstract class DbSubscription extends Subscription
{
  private final String tableName_;
  
  public DbSubscription(JsonObject<?> json, INameFactory nameFactory)
  {
    super(json, 1000);
    
    tableName_ = nameFactory.getTableName(json.getRequiredString("tableName")).toString();
  }

  public String getTableName()
  {
    return tableName_;
  }
}
