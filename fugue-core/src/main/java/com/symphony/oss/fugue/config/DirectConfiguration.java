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

package com.symphony.oss.fugue.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.symphony.oss.commons.fault.FaultAccumulator;
import com.symphony.oss.commons.fluent.BaseAbstractBuilder;

/**
 * A minimal implementation of IConfiguration from in-memory values.
 * 
 * @author Bruce Skingle
 *
 */
public class DirectConfiguration extends Configuration implements IConfiguration
{
  DirectConfiguration(AbstractBuilder<?,?> builder)
  {
    super(builder.createConfig());
  }
  
  abstract static class AbstractBuilder<T extends AbstractBuilder<T,B>, B extends DirectConfiguration> extends BaseAbstractBuilder<T,B>
  {
    String environmentTypeId_ = "dev";
    String environmentId_ = "master";
    String regionId_ = "us-east-1";
    String serviceId_;
    
    AbstractBuilder(Class<T> type)
    {
      super(type);
    }

    public T withEnvironmentTypeId(String environmentTypeId)
    {
      environmentTypeId_ = environmentTypeId;
      
      return self();
    }

    public T withEnvironmentId(String environmentId)
    {
      environmentId_ = environmentId;
      
      return self();
    }

    public T withRegionId(String regionId)
    {
      regionId_ = regionId;
      
      return self();
    }

    public T withServiceId(String serviceId)
    {
      serviceId_ = serviceId;
      
      return self();
    }

    @Override
    protected void validate(FaultAccumulator faultAccumulator)
    {
      super.validate(faultAccumulator);
      
      faultAccumulator.checkNotNull("Service ID", serviceId_);
    }
    
    JsonNode createConfig()
    {
      ObjectNode tree = new ObjectMapper().createObjectNode();
      
      ObjectNode idNode = tree.objectNode();
      
      tree.set("id", idNode);

      idNode.set("environmentType", idNode.textNode(environmentTypeId_));
      idNode.set("environmentId", idNode.textNode(environmentId_));
      idNode.set("regionId", idNode.textNode(regionId_));
      idNode.set("serviceId", idNode.textNode(serviceId_));
      
      return tree;
    }
  }
  
  /**
   * Builder.
   * 
   * @author Bruce Skingle
   *
   */
  public static class Builder extends AbstractBuilder<Builder, DirectConfiguration>
  {
    /**
     * Constructor.
     */
    public Builder()
    {
      super(Builder.class);
    }

    @Override
    protected DirectConfiguration construct()
    {
      return new DirectConfiguration(this);
    }
  }
}
