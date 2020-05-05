/*
 *
 *
 * Copyright 2020 Symphony Communication Services, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.symphony.oss.fugue.server.http;

import java.util.List;

import com.symphony.oss.fugue.server.IFugueServer;

/**
 * A fluent HTTP container of Fugue components.
 * 
 * @author Bruce Skingle
 */
public interface IFugeHttpServer extends IFugueServer
{
  /**
   * Return the http port in use.
   * 
   * @return the http port in use.
   */
  int getHttpPort();
  
  /**
   * Return all of the registered components which implement IServletProvider.
   * 
   * @return all of the registered components which implement IServletProvider.
   */
  List<IServletProvider> getServletProviders();

  /**
   * Return all of the registered components which implement IUrlPathServlet.
   * 
   * @return all of the registered components which implement IUrlPathServlet.
   */
  List<IUrlPathServlet> getServlets();

  /**
   * Return the URL for the server.
   * 
   * @return The URL for the server.
   */
  String getServerUrl();
}
