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

package com.symphony.oss.fugue.server;

import com.symphony.oss.fugue.container.IFugueComponentRegistry;

public class FugueServerAssembly
{
  private FugueServer.Builder builder_;
  private FugueServer server_;
  
  public FugueServerAssembly(String applicationName)
  {
    builder_ = new FugueServer.Builder()
        .withApplicationName(applicationName)
        ;
  }

  public IFugueComponentRegistry getRegistry()
  {
    return builder_;
  }

  protected void setBuilder(FugueServer.Builder builder)
  {
    builder_ = builder;
  }

  public <C> C register(C component)
  {
    return builder_.register(component);
  }

  public synchronized final FugueServer getServer()
  {
    if(server_ == null)
      server_ = builder_.build();
    
    return server_;
  }
}
