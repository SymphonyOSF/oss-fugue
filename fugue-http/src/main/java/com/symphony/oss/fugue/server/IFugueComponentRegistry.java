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

public interface IFugueComponentRegistry
{
  /**
   * Register the given component.
   * 
   * The container is aware of a number of types of component and treats them appropriately.
   * 
   * This method returns its parameter so that it can be called from a constructor assignment, e.g:
   * 
   * <code>nameFactory_ = register(new SystemNameFactory(config_));</code>
   * 
   * @param <C>       The type of the component to be registered. 
   * @param component The component to be registered.
   * 
   * @return The component.
   */
  <C> C register(C component);
}
