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

package com.symphony.oss.fugue.config;

import java.util.List;

/**
 * A class which delegates all IConfiguration methods to another IConfiguration.
 * 
 * This is useful to subclass and add specific getters.
 * 
 * @author Bruce Skingle
 *
 */
public class ConfigurationDelegator implements IConfiguration
{
  private final IConfiguration delegate_;

  /**
   * Constructor.
   * 
   * @param delegate A delegate.
   */
  public ConfigurationDelegator(IConfiguration delegate)
  {
    delegate_ = delegate;
  }

  @Override
  public IConfiguration getConfiguration(String name)
  {
    return delegate_.getConfiguration(name);
  }

  @Override
  @Deprecated
  public String getString(String name) throws NoSuchConfigException
  {
    return delegate_.getString(name);
  }

  @Override
  public String getString(String name, String defaultValue)
  {
    return delegate_.getString(name, defaultValue);
  }

  @Override
  public String getRequiredString(String name)
  {
    return delegate_.getRequiredString(name);
  }

  @Override
  public long getRequiredLong(String name)
  {
    return delegate_.getRequiredLong(name);
  }

  @Override
  public boolean getBoolean(String name) throws NoSuchConfigException
  {
    return delegate_.getBoolean(name);
  }

  @Override
  public boolean getBoolean(String name, boolean defaultValue)
  {
    return delegate_.getBoolean(name, defaultValue);
  }

  @Override
  public boolean getRequiredBoolean(String name)
  {
    return delegate_.getRequiredBoolean(name);
  }

  @Override
  @Deprecated
  public List<String> getStringArray(String name) throws NoSuchConfigException
  {
    return delegate_.getStringArray(name);
  }

  @Override
  @Deprecated
  public List<String> getRequiredStringArray(String name)
  {
    return delegate_.getRequiredStringArray(name);
  }

  @Override
  public String getName()
  {
    return delegate_.getName();
  }

  @Override
  public long getLong(String name, long defaultValue)
  {
    return delegate_.getLong(name, defaultValue);
  }

  @Override
  public int getRequiredInt(String name)
  {
    return delegate_.getRequiredInt(name);
  }

  @Override
  public int getInt(String name, int defaultValue)
  {
    return delegate_.getInt(name, defaultValue);
  }

  @Override
  public Integer getInteger(String name, Integer defaultValue)
  {
    return delegate_.getInteger(name, defaultValue);
  }

  @Override
  public List<String> getListOfString(String name, List<String> defaultValue)
  {
    return delegate_.getListOfString(name, defaultValue);
  }

  @Override
  public List<String> getRequiredListOfString(String name)
  {
    return delegate_.getRequiredListOfString(name);
  }
  
}
