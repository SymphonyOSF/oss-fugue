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

package com.symphony.oss.fugue.pubsub;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.symphony.oss.commons.concurrent.NamedThreadFactory;
import com.symphony.oss.fugue.Fugue;
import com.symphony.oss.fugue.config.IConfiguration;
import com.symphony.oss.fugue.counter.IBusyCounter;
import com.symphony.oss.fugue.counter.ITopicBusyCounterFactory;
import com.symphony.oss.fugue.naming.Name;
import com.symphony.oss.fugue.naming.SubscriptionName;

/**
 * Base class for synchronous pull type implementations.
 * 
 * @author Bruce Skingle
 *
 * @param <T> Type of concrete manager, needed for fluent methods.
 */
public abstract class AbstractPullSubscriberManager<P, T extends AbstractPullSubscriberManager<P,T>> extends AbstractSubscriberManager<P,T>
{
  private static final Logger                 log_           = LoggerFactory
      .getLogger(AbstractPullSubscriberManager.class);

  private final ITopicBusyCounterFactory      busyCounterFactory_;

  private int                                 subscriberThreadPoolSize_;
  private int                                 handlerThreadPoolSize_;
  private final LinkedBlockingQueue<Runnable> executorQueue_ = new LinkedBlockingQueue<Runnable>();
  private final LinkedBlockingQueue<Runnable> handlerQueue_  = new LinkedBlockingQueue<Runnable>();
  private ThreadPoolExecutor                  subscriberExecutor_;
  private ThreadPoolExecutor                  handlerExecutor_;

  
  protected AbstractPullSubscriberManager(Builder<?,P,T> builder)
  {
    super(builder);
    
    busyCounterFactory_  = builder.busyCounterFactory_;
    
    IConfiguration subscriberConfig = config_.getConfiguration(builder.getConfigPath());
    
    subscriberThreadPoolSize_ = subscriberConfig.getInt("subscriberThreadPoolSize", 4);
    handlerThreadPoolSize_ = subscriberConfig.getInt("handlerThreadPoolSize", 9 * subscriberThreadPoolSize_);

//    subscriberThreadPoolSize_ = 4; //8 * getTotalSubscriptionCnt();
  }

  /**
   * Builder.
   * 
   * @author Bruce Skingle
   *
   * @param <T>   The concrete type returned by fluent methods.
   * @param <B>   The concrete type of the built object.
   */
  public static abstract class Builder<T extends Builder<T,P,B>, P, B extends AbstractPullSubscriberManager<P,B>>
  extends AbstractSubscriberManager.Builder<T,P,B>
  implements IPullSubscriberManagerBuilder<T,P,B>
  {
    private ITopicBusyCounterFactory         busyCounterFactory_;

    protected Builder(Class<T> type)
    {
      super(type);
    }
    
    @Override
    public T withBusyCounterFactory(ITopicBusyCounterFactory busyCounterFactory)
    {
      busyCounterFactory_ = busyCounterFactory;
      
      return self();
    }
    
    protected abstract String getConfigPath();
  }

  
  protected ITopicBusyCounterFactory getBusyCounterFactory()
  {
    return busyCounterFactory_;
  }

  @Override
  public void start()
  {
    if(totalSubscriptionCnt_ == 0)
    {
      log_.info("No subscriptions, not starting.");
      return;
    }
    
    if(Fugue.isDebugSingleThread())
    {
      subscriberThreadPoolSize_ = totalSubscriptionCnt_ == 0 ? 1 : totalSubscriptionCnt_;
      handlerThreadPoolSize_ = 1;
    }
    else
    {
      int min = getTotalSubscriptionCnt() * 2;
      
      if(subscriberThreadPoolSize_ < min)
      { 
        log_.warn("Configured for " + subscriberThreadPoolSize_ +
          " subscriber threads for a total of " +
          getTotalSubscriptionCnt() + " subscriptions, using " + min + " subscriber threads");
        
        subscriberThreadPoolSize_ = min;
      }
      
      min = subscriberThreadPoolSize_ * 2;
      
      if(handlerThreadPoolSize_ < min)
      { 
        log_.warn("Configured for " + handlerThreadPoolSize_ + " handler threads for " +
          subscriberThreadPoolSize_ + " subscriber treads, using " + min + " handler threads");
        
        handlerThreadPoolSize_ = min;
      }
    }
    
    log_.info("Starting AbstractPullSubscriberManager with " + subscriberThreadPoolSize_ +
        " subscriber threads and " + handlerThreadPoolSize_ + " handler threads for a total of " +
        getTotalSubscriptionCnt() + " subscriptions...");

    subscriberExecutor_ = new ThreadPoolExecutor(subscriberThreadPoolSize_, subscriberThreadPoolSize_,
        10000L, TimeUnit.MILLISECONDS,
        executorQueue_, new NamedThreadFactory("PubSub-subscriber"));
    
    handlerExecutor_ = new ThreadPoolExecutor(handlerThreadPoolSize_, handlerThreadPoolSize_,
        10000L, TimeUnit.MILLISECONDS,
        handlerQueue_, new NamedThreadFactory("PubSub-handler", true));
      
    super.start();
  }
  
  @Override
  protected void stopSubscriptions()
  {
    if(subscriberExecutor_ != null)
      subscriberExecutor_.shutdown();
    
    if(handlerExecutor_ != null)
      handlerExecutor_.shutdown();
      
    if(subscriberExecutor_ != null)
      stop(subscriberExecutor_, 60);
    
    if(handlerExecutor_ != null)
      stop(handlerExecutor_, 10);
  }

  private void stop(ThreadPoolExecutor executor, int delay)
  {
    try {
      // Wait a while for existing tasks to terminate
      if (!executor.awaitTermination(delay, TimeUnit.SECONDS)) {
        executor.shutdownNow(); // Cancel currently executing tasks
      // Wait a while for tasks to respond to being cancelled
      if (!executor.awaitTermination(delay, TimeUnit.SECONDS))
          System.err.println("Pool did not terminate");
        }
      } catch (InterruptedException ie) {
        // (Re-)Cancel if current thread also interrupted
      executor.shutdownNow();
      // Preserve interrupt status
        Thread.currentThread().interrupt();
    }
  }

  protected void submit(Runnable subscriber, boolean force)
  {
    if(force || executorQueue_.size() < subscriberThreadPoolSize_)
      subscriberExecutor_.submit(subscriber);
  }

  protected void printQueueSize()
  {
    log_.debug("Queue size " + executorQueue_.size());
  }
  
//  public IBatch newBatch()
//  {
//    return new ExecutorBatch(handlerExecutor_);
//  }

  ThreadPoolExecutor getHandlerExecutor()
  {
    return handlerExecutor_;
  }
  
  protected IBusyCounter createBusyCounter(Name subscriptionName)
  {
    if(getBusyCounterFactory() == null)
      return null;
    
    if(subscriptionName instanceof SubscriptionName)
    {
      return getBusyCounterFactory().create(((SubscriptionName)subscriptionName).getTopicName().getTopicId());
    }
    
    throw new IllegalStateException("BusyCounter can only be set with SubscriptionName");
  }
}
