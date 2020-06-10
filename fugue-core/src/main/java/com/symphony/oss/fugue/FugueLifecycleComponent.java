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

package com.symphony.oss.fugue;

import com.symphony.oss.commons.fluent.BaseAbstractBuilder;

/**
 * A base class for Fugue Lifecycle Components.
 * 
 * @author Bruce Skingle
 */
public abstract class FugueLifecycleComponent extends FugueLifecycleBase implements IFugueLifecycleComponent
{
  private FugueComponentState componentState_         = FugueComponentState.OK;
  private String              componentStatusMessage_ = "OK";

  /**
   * Constructor.
   * 
   * @param builder Builder object.
   */
  protected FugueLifecycleComponent(AbstractBuilder<?,?> builder)
  {
  }
  
  /**
   * The builder implementation.
   * 
   * Any sub-class of FugueLifecycleComponent would need to implement its own Abstract sub-class of this class
   * and then a concrete Builder class which is itself a sub-class of that.
   * 
   * @author Bruce Skingle
   *
   * @param <T> The type of the concrete Builder
   * @param <B> The type of the built class, some subclass of FugueLifecycleComponent
   */
  protected static abstract class AbstractBuilder<T extends AbstractBuilder<T,B>, B extends IFugueLifecycleComponent>
  extends BaseAbstractBuilder<T, B>
  {
    protected AbstractBuilder(Class<T> type)
    {
      super(type);
    }
  }

  @Override
  public FugueComponentState getComponentState()
  {
    return componentState_;
  }

  @Override
  public String getComponentStatusMessage()
  {
    return componentStatusMessage_;
  }

  protected void setComponentState(FugueComponentState componentState)
  {
    componentState_ = componentState;
  }

  protected void setComponentStatusMessage(String componentStatusMessage)
  {
    componentStatusMessage_ = componentStatusMessage;
  }
}
