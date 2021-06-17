/*
 *
 *
 * Copyright 2019 Symphony Communication Services, LLC.
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

package com.symphony.oss.fugue.inmemory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.symphony.oss.commons.dom.json.IImmutableJsonDomNode;
import com.symphony.oss.commons.dom.json.jackson.JacksonAdaptor;
import com.symphony.oss.commons.fluent.BaseAbstractBuilder;
import com.symphony.oss.fugue.naming.CredentialName;
import com.symphony.oss.fugue.secret.ISecretManager;
import com.symphony.oss.fugue.secret.SecretNotFoundException;

/**
 * In Memory implementation of Secret Manager.
 * 
 * @author Bruce Skingle
 *
 */
public class InMemorySecretManager implements ISecretManager
{
  private final Map<String, IImmutableJsonDomNode> secretMap_ = new HashMap<>();

  /**
   * Constructor.
   * 
   * @param builder A builder, follows the same pattern as the real implementations.
   */
  private InMemorySecretManager(Builder builder)
  {
  }
  
  
  /**
   * Builder for AwsSecretManager.
   * 
   * @author Bruce Skingle
   *
   */
  public static class Builder extends BaseAbstractBuilder<Builder, InMemorySecretManager>
  {
    /**
     * Constructor.
     */
    public Builder()
    {
      super(Builder.class);
    }
    
    @Override
    protected InMemorySecretManager construct()
    {
      return new InMemorySecretManager(this);
    }
  }
  
  @Override
  public IImmutableJsonDomNode getSecret(CredentialName name) throws SecretNotFoundException
  {
    return getSecret(name.toString());
  }
  
  @Override
  public IImmutableJsonDomNode getSecret(String name) throws SecretNotFoundException
  {
    IImmutableJsonDomNode result = secretMap_.get(name);
    
    if(result == null)
      throw new SecretNotFoundException("Unable to find secret " + name);
    
    return result;
  }
  
  @Override
  public String getSecretAsString(CredentialName name) throws SecretNotFoundException
  {
    return getSecretAsString(name.toString());
  }
  
  @Override
  public String getSecretAsString(String name) throws SecretNotFoundException
  {
    IImmutableJsonDomNode result = secretMap_.get(name);
    
    if(result == null)
      throw new SecretNotFoundException("Unable to find secret " + name);
    
    return result.toString();
  }
  
  @Override
  public void putSecret(CredentialName name, IImmutableJsonDomNode secret)
  {
    secretMap_.put(name.toString(), secret);
  }

  @Override
  public void putSecret(CredentialName name, String secret)
  {
    try
    {
      JsonNode tree = new ObjectMapper().readTree(secret);
      IImmutableJsonDomNode domNode = JacksonAdaptor.adapt(tree).immutify();
      
      putSecret(name, domNode);
    }
    catch (IOException e)
    {
      throw new IllegalArgumentException("Invalid JSON", e);
    }
  }
}
