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

package com.symphony.oss.fugue.counter;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.symphony.oss.fugue.config.IConfiguration;

public class BusyCounter implements IBusyCounter
{
  private static final Logger log_        = LoggerFactory.getLogger(BusyCounter.class);

  private AtomicInteger       busyCycles_ = new AtomicInteger();
  private AtomicInteger       idleCycles_ = new AtomicInteger();

  private int                 busyLevel_;
  private int                 busyLimit_;
  private int                 idleLimit_;
  private long                coolDown_;
  private long                lastEvent_;

  
  public BusyCounter(int busyLevel, int busyLimit, int idleLimit, long coolDown)
  {
    busyLevel_ = busyLevel;
    busyLimit_ = busyLimit;
    idleLimit_ = idleLimit;
    coolDown_ = coolDown;
  }
  
  public BusyCounter(IConfiguration config)
  {
    this(
        config.getInt("busyLevel", 5),
        config.getInt("busyLimit", 5),
        config.getInt("idleLimit", 3),
        config.getInt("coolDown", 20000)
        );
  }

  @Override
  public ScaleAction busy(int count)
  {
    if(count >= busyLevel_)
      return busy() ? ScaleAction.ScaleUp : ScaleAction.DoNothing;
    else
      return idle() ? ScaleAction.ScaleDown : ScaleAction.DoNothing;
  }

  private boolean busy()
  {
    idleCycles_.set(0);
    int busyCnt = busyCycles_.incrementAndGet();
    
    if(busyCnt >= busyLimit_)
      return doScaleUp();
    
    return false;
  }

  private boolean idle()
  {
    busyCycles_.set(0);
    int idleCnt = idleCycles_.incrementAndGet();
    
    if(idleCnt >= idleLimit_)
      return doScaleDown();
    
    return false;
  }

  protected boolean doScaleUp()
  {
    if(checkCoolDown())
    {
      log_.info("Scale UP");
      return scaleUp();
    }
    return false;
  }

  /**
   * Scale up
   * @return true iff the caller should scale up, i.e. if this call FAILS
   */
  protected boolean scaleUp()
  {
    return true; 
  }

  protected boolean doScaleDown()
  {
    if(checkCoolDown())
    {
      log_.info("Scale DOWN");
      return scaleDown();
    }
    return false;
  }

  protected boolean scaleDown()
  {
    return false;
  }

  private synchronized boolean checkCoolDown()
  {
    long now = System.currentTimeMillis();
    
    if(now - lastEvent_ > coolDown_)
    {
      lastEvent_ = now;
      idleCycles_.set(0);
      busyCycles_.set(0);
      
      return true;
    }
    return false;
  }

}
