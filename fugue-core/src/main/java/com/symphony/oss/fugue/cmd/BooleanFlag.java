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

package com.symphony.oss.fugue.cmd;

import java.util.function.Consumer;

import com.symphony.oss.fugue.cmd.CommandLineHandler.ArrayIterator;

public class BooleanFlag extends AbstractFlag
{
  private final Consumer<Boolean> setter_;

  public BooleanFlag(Character shortFlag, String longFlag, String envName, Consumer<Boolean> setter)
  {
    super(shortFlag, longFlag, envName);

    setter_ = setter;
  }

  public void set(Boolean value)
  {
    setter_.accept(value);
  }

  @Override
  public boolean isMultiple()
  {
    return false;
  }

  @Override
  public boolean isRequired()
  {
    return false;
  }

  @Override
  public void process(ArrayIterator it, boolean boolVal)
  {
    set(boolVal);
  }

  @Override
  public void process(String value)
  {
    set("true".equalsIgnoreCase(value));
  }
}
