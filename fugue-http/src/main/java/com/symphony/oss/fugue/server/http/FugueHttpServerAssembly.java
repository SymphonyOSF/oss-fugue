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

import com.symphony.oss.fugue.IFugueApplication;
import com.symphony.oss.fugue.server.IFugueComponentRegistry;

public class FugueHttpServerAssembly
{
  private FugueHttpServer.Builder builder_;
  private FugueHttpServer server_;

  public FugueHttpServerAssembly(IFugueApplication application)
  {
    builder_ = new FugueHttpServer.Builder()
        .withFugueApplication(application);
  }
  
  public FugueHttpServerAssembly(String applicationName, int httpPort)
  {
    builder_ = new FugueHttpServer.Builder()
        .withApplicationName(applicationName)
        .withHttpPort(httpPort)
        ;
  }

  public IFugueComponentRegistry getRegistry()
  {
    return builder_;
  }

  protected void setBuilder(FugueHttpServer.Builder builder)
  {
    builder_ = builder;
  }

  public <C> C register(C component)
  {
    return builder_.register(component);
  }

  public synchronized final FugueHttpServer getServer()
  {
    if(server_ == null)
      server_ = builder_.build();
    
    return server_;
  }
//
//  public FugueHttpComponentContainer start()
//  {
//    return server_.start();
//  }
//
//  public FugueHttpComponentContainer quiesce()
//  {
//    return server_.quiesce();
//  }
//
//  public FugueHttpComponentContainer stop()
//  {
//    return server_.stop();
//  }
  
  
}
