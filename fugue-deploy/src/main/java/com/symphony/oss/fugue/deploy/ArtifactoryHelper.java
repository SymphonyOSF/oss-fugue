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

package com.symphony.oss.fugue.deploy;

import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.symphony.oss.commons.dom.json.ImmutableJsonObject;
import com.symphony.oss.commons.dom.json.MutableJsonObject;

/**
 * Base class for config helpers and providers.
 * 
 * @author Geremia Longobardo
 *
 */
public class ArtifactoryHelper
{
  protected String username_;
  protected String password_;
  
  private FugueDeploy fugueDeploy_;
  
  private static final String DOT_JAR   = ".jar";

  /**
   * Init method, allows program parameters to be added.
   * 
   * @param fugueDeploy the main program to which parameters may be added.
   */
  public void init(FugueDeploy fugueDeploy)
  {
    fugueDeploy_ = fugueDeploy;
  }
 
  
  protected String getService()
  {
    return fugueDeploy_.getService();
  }

  protected String getEnvironment()
  {
    return fugueDeploy_.getEnvironment();
  }

  protected String getRegion()
  {
    return fugueDeploy_.getRegion();
  }

  protected String getEnvironmentType()
  {
    return fugueDeploy_.getEnvironmentType();
  }

  public void fetchArtifact(String path, String name, String buildId, OutputStream out)
  {
  }


  public void setCredentials(String username, String password)
  {
    username_ = username;
    password_ = password;    
  }


  public static String getFilename(String name, String buildId)
  {
    return  name + "-" + buildId + DOT_JAR;
  }
}
