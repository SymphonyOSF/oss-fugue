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

package org.symphonyoss.s2.fugue.aws.assembly;

import org.symphonyoss.s2.fugue.aws.config.S3Configuration;
import org.symphonyoss.s2.fugue.aws.sts.StsManager;

import com.symphony.oss.commons.fault.FaultAccumulator;
import com.symphony.oss.commons.fluent.BaseAbstractBuilder;
import com.symphony.oss.fugue.IFugueAssembly;
import com.symphony.oss.fugue.config.GlobalConfiguration;
import com.symphony.oss.fugue.config.IGlobalConfiguration;
import com.symphony.oss.fugue.naming.INameFactory;
import com.symphony.oss.fugue.naming.NameFactory;
import com.symphony.oss.fugue.server.IFugeComponentContainer;
import com.symphony.oss.fugue.server.IFugueComponentRegistry;

/**
 * A base assembly for AWS implementations.
 * 
 * @author Bruce Skingle
 *
 */
public class AwsFugueAssembly implements IFugueAssembly
{
  protected final IFugueComponentRegistry    container_;
  protected final IGlobalConfiguration       config_;
  protected final INameFactory               nameFactory_;
  protected final String                     region_;
  protected final StsManager                 stsManager_;
  
  protected AwsFugueAssembly(AbstractBuilder<?,?> builder)
  {
    container_                = builder.container_;
    config_                   = builder.config_;
    nameFactory_              = builder.nameFactory_;
    
    region_                   = builder.region_;
    stsManager_               = builder.stsManager_;
  }
  
  protected <C> C register(C component)
  {
    return container_.register(component);
  }
  
  /**
   * @return The global configuration for this assembly.
   */
  public IGlobalConfiguration getConfiguration()
  {
    return config_;
  }

  /**
   * @return The name factory for this assembly.
   */
  public INameFactory getNameFactory()
  {
    return nameFactory_;
  }
  
  protected static abstract class AbstractBuilder<T extends AbstractBuilder<T,B>, B extends AwsFugueAssembly>
  extends BaseAbstractBuilder<T, B>
  {
    protected IGlobalConfiguration              config_;
    protected String                            serviceId_;
    protected INameFactory                      nameFactory_;
    protected String                            region_;
    protected StsManager                        stsManager_;

    protected IFugueComponentRegistry           container_;
    
    public AbstractBuilder(Class<T> type)
    {
      super(type);
    }

    public T withConfiguration(IGlobalConfiguration config)
    {
      config_ = config;
      
      return self();
    }

    public T withServiceId(String serviceId)
    {
      serviceId_ = serviceId;
      
      return self();
    }

    public T withNameFactory(INameFactory nameFactory)
    {
      nameFactory_ = nameFactory;
      
      return self();
    }
    
    public IGlobalConfiguration getConfiguration()
    {
      return config_;
    }

    public String getRegion()
    {
      return region_;
    }

    public INameFactory getNameFactory()
    {
      return nameFactory_;
    }

    public T withContainer(IFugueComponentRegistry container)
    {
      container_ = container;
      
      return self();
    }

    @Override
    public void validate(FaultAccumulator faultAccumulator)
    {
      super.validate(faultAccumulator);
      
      faultAccumulator.checkNotNull(container_, "container");
      
      if(config_ == null)
        config_ = new GlobalConfiguration(S3Configuration.FACTORY.newInstance());
      
      if(nameFactory_ == null)
      {
        nameFactory_ = new NameFactory(config_);
        
        if(serviceId_ != null)
          nameFactory_ = nameFactory_.withServiceId(serviceId_);
      }
      
      region_                   = config_.getCloudRegionId(); //.getAmazonConfiguration().getRegionName();
      stsManager_               = new StsManager(region_);
      
    }
  }
}
