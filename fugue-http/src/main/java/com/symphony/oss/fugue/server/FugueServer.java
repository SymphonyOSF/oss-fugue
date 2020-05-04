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

package com.symphony.oss.fugue.server;

import com.symphony.oss.fugue.server.AbstractFugueServer.AbstractBuilder;

/**
 * The main component for a Fugue process.
 * 
 * @author Bruce Skingle
 *
 */
public class FugueServer implements IFugueComponentRegistry
{ 
  private AbstractBuilder<?, ?> builder_;
  private AbstractFugueServer<?> server_;

  protected FugueServer(AbstractFugueServer.AbstractBuilder<?, ?> builder)
  {
    builder_ = builder;
  }

  public <C> C register(C component)
  {
    return builder_.register(component);
  }

  public synchronized final AbstractFugueServer<?> getServer()
  {
    if(server_ == null)
      server_ = builder_.build();
    
    return server_;
  }

  public FugueServer start()
  {
    getServer().start();
    
    return this;
  }

  public FugueServer quiesce()
  {
    getServer().quiesce();
    
    return this;
  }

  public FugueServer stop()
  {
    getServer().stop();
    
    return this;
  }
  
  class FugueServerImpl extends AbstractFugueServer<FugueServerImpl>
  { 
    protected FugueServerImpl(Builder builder)
    {
      super(FugueServerImpl.class, builder);
    }

    public class Builder extends AbstractBuilder<Builder, FugueServerImpl>
    {
      protected Builder()
      {
        super(Builder.class);
      }

      @Override
      protected FugueServerImpl construct()
      {
        return new FugueServerImpl(this);
      }
    }
  }
}
