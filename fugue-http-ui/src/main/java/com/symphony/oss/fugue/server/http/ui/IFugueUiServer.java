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

package com.symphony.oss.fugue.server.http.ui;

import java.util.List;

import com.symphony.oss.fugue.server.http.IFugeHttpServer;
import com.symphony.oss.fugue.server.http.IResourceProvider;
import com.symphony.oss.fugue.server.http.ui.servlet.ICommand;

/**
 * The main component for a Fugue process.
 * 
 * @author Bruce Skingle
 *
 */
public interface IFugueUiServer extends IFugeHttpServer
{
  /**
   * @return all of the registered components which implement IResourceProvider.
   */
  List<IResourceProvider> getResourceProviders();

  /**
   * @return all of the registered components which implement ICommand.
   */
  List<ICommand> getCommands();
}
