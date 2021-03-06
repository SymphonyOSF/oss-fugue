/*
 *
 *
 * Copyright 2017-2018 Symphony Communication Services, LLC.
 *
 * Licensed to The Symphony Software Foundation (SSF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The SSF licenses this file
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

package com.symphony.oss.fugue.server.http.ui.servlet;

import java.util.EnumSet;

import com.symphony.oss.fugue.FugueLifecycleState;


public class Command implements ICommand
{
  private final String                       name_;
  private final String                       path_;
  private final EnumSet<FugueLifecycleState> validStates_;
  private final ICommandHandler              handler_;
  private final boolean                      closeWindow_;
  
  public Command(String name, String path, ICommandHandler handler, boolean closeWindow)
  {
    this(name, path, EnumSet.allOf(FugueLifecycleState.class), handler, closeWindow);
  }
  
  public Command(String name, String path, EnumSet<FugueLifecycleState> validStates, ICommandHandler handler, boolean closeWindow)
  {
    name_ = name;
    path_ = path;
    validStates_ = validStates;
    handler_ = handler;
    closeWindow_ = closeWindow;
  }

  @Override
  public String getName()
  {
    return name_;
  }

  @Override
  public String getPath()
  {
    return path_;
  }

  @Override
  public EnumSet<FugueLifecycleState> getValidStates()
  {
    return validStates_;
  }

  @Override
  public ICommandHandler getHandler()
  {
    return handler_;
  }

  @Override
  public boolean isCloseWindow()
  {
    return closeWindow_;
  }
  
 
}
