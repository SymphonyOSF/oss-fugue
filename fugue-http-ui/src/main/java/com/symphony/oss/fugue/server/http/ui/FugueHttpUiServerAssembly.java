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

package com.symphony.oss.fugue.server.http.ui;

import com.symphony.oss.fugue.IFugueApplication;
import com.symphony.oss.fugue.container.IFugueComponentRegistry;

public class FugueHttpUiServerAssembly
{
  private FugueHttpUiServer.Builder builder_;
  private FugueHttpUiServer         server_;

  public FugueHttpUiServerAssembly(IFugueApplication application)
  {
    builder_ = new FugueHttpUiServer.Builder()
        .withFugueApplication(application);
  }
  
  public FugueHttpUiServerAssembly(String applicationName, int httpPort)
  {
    builder_ = new FugueHttpUiServer.Builder()
        .withApplicationName(applicationName)
        .withHttpPort(httpPort)
        ;
  }

  public IFugueComponentRegistry getRegistry()
  {
    return builder_;
  }

  public FugueHttpUiServer.Builder getBuilder()
  {
    return builder_;
  }

  protected void setBuilder(FugueHttpUiServer.Builder builder)
  {
    builder_ = builder;
  }

  public <C> C register(C component)
  {
    return builder_.register(component);
  }

  public synchronized final FugueHttpUiServer getServer()
  {
    if(server_ == null)
      server_ = builder_.build();
    
    return server_;
  }
}
