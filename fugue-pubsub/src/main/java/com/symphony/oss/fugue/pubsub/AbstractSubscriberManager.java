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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.symphony.oss.commons.fault.FaultAccumulator;
import com.symphony.oss.fugue.FugueLifecycleComponent;
import com.symphony.oss.fugue.FugueLifecycleState;
import com.symphony.oss.fugue.config.IConfiguration;
import com.symphony.oss.fugue.counter.ICounter;
import com.symphony.oss.fugue.naming.INameFactory;
import com.symphony.oss.fugue.pipeline.FatalConsumerException;
import com.symphony.oss.fugue.pipeline.IThreadSafeErrorConsumer;
import com.symphony.oss.fugue.pipeline.IThreadSafeRetryableConsumer;
import com.symphony.oss.fugue.pipeline.RetryableConsumerException;
import com.symphony.oss.fugue.trace.ITraceContext;
import com.symphony.oss.fugue.trace.ITraceContextTransactionFactory;

/**
 * Base class for subscriber managers.
 * 
 * @author Bruce Skingle
 *
 * @param <P> Type of the payload.
 * @param <T> Type of concrete manager, needed for fluent methods.
 */
public abstract class AbstractSubscriberManager<P, T extends AbstractSubscriberManager<P,T>> extends FugueLifecycleComponent
implements ISubscriberManager
{
  protected static final long          FAILED_DEAD_LETTER_RETRY_TIME = TimeUnit.HOURS.toMillis(1);
  protected static final long          FAILED_CONSUMER_RETRY_TIME    = TimeUnit.SECONDS.toMillis(30);
  protected static final long          MESSAGE_PROCESSED_OK          = -1;

  private static final Logger                     log_                          = LoggerFactory.getLogger(AbstractSubscriberManager.class);
  private static final Integer                    FAILURE_CNT_LIMIT             = 25;

  protected final INameFactory                    nameFactory_;
  protected final ImmutableList<ISubscription<P>> subscribers_;
  protected final ITraceContextTransactionFactory traceFactory_;
  protected final IThreadSafeErrorConsumer<P>     unprocessableMessageConsumer_;
  protected final IConfiguration                  config_;
  protected final ICounter                        counter_;
  protected final int                             totalSubscriptionCnt_;

  // TODO: replace this with a local topic
  private Cache<String, Integer>            failureCache_                 = CacheBuilder.newBuilder()
                                                                            .maximumSize(5000)
                                                                            .expireAfterAccess(30, TimeUnit.MINUTES)
                                                                            .build();
  

  protected AbstractSubscriberManager(Builder<?,P,T> builder)
  {
    super(builder);
    
    nameFactory_                  = builder.nameFactory_;
    subscribers_                  = ImmutableList.copyOf(builder.subscribers_);
    traceFactory_                 = builder.traceFactory_;
    unprocessableMessageConsumer_ = builder.unprocessableMessageConsumer_;
    config_                       = builder.config_;
    counter_                      = builder.counter_;
    totalSubscriptionCnt_         = builder.totalSubscriptionCnt_;
  }

  @Override
  public int getTotalSubscriptionCnt()
  {
    return totalSubscriptionCnt_;
  }

  /**
   * Builder.
   * 
   * @author Bruce Skingle
   *
   * @param <T>   The concrete type returned by fluent methods.
   * @param <B>   The concrete type of the built object.
   */
  protected static abstract class Builder<T extends Builder<T,P,B>, P, B extends AbstractSubscriberManager<P,B>>
  extends FugueLifecycleComponent.AbstractBuilder<T,B>
  implements ISubscriberManagerBuilder<T,P,B>
  {
    protected INameFactory                     nameFactory_;
    protected List<ISubscription<P>>           subscribers_ = new LinkedList<>();
    protected ITraceContextTransactionFactory  traceFactory_;
    protected IThreadSafeErrorConsumer<P>      unprocessableMessageConsumer_;
    protected IConfiguration                   config_;
    protected ICounter                         counter_;
    protected int                              totalSubscriptionCnt_;

    protected Builder(Class<T> type)
    {
      super(type);
    }
    
    @Override
    public T withNameFactory(INameFactory nameFactory)
    {
      nameFactory_ = nameFactory;
      
      return self();
    }

//    @Override
//    public T withSubscription(IThreadSafeRetryableConsumer<String> consumer, String subscriptionName)
//    {
//      return super.withSubscription(consumer, subscriptionName);
//    }
//
//    @Override
//    public T withSubscription(IThreadSafeRetryableConsumer<String> consumer, Subscription subscription)
//    {
//      return super.withSubscription(consumer, subscription);
//    }
//  
//    @Override
//    public T withSubscription(IThreadSafeRetryableConsumer<String> consumer, String subscriptionId, String topicId,
//        String... additionalTopicIds)
//    {
//      return super.withSubscription(consumer, subscriptionId, topicId, additionalTopicIds);
//    }
//  
//    @Override
//    public T withSubscription(IThreadSafeRetryableConsumer<String> consumer, String subscriptionId, Collection<TopicName> topicNames)
//    {
//      return super.withSubscription(consumer, subscriptionId, topicNames);
//    }
//    
//    @Override
//    public T withSubscription(IThreadSafeRetryableConsumer<String> consumer, String subscriptionId, String[] topicNames)
//    {
//      return super.withSubscription(consumer, subscriptionId, topicNames);
//    }
    


    @Override
    public T withSubscription(ISubscription<P> subscription)
    {
      subscribers_.add(subscription);
      
      totalSubscriptionCnt_ += subscription.getSubscriptionNames().size();
      
      return self();
    }

    @Override
    public T withConfig(IConfiguration config)
    {
      config_ = config;
      
      return self();
    }

    @Override
    public T withCounter(ICounter counter)
    {
      counter_ = counter;
      
      return self();
    }

    @Override
    public T withTraceContextTransactionFactory(ITraceContextTransactionFactory traceFactory)
    {
      traceFactory_ = traceFactory;
      
      return self();
    }

    public ITraceContextTransactionFactory getTraceContextTransactionFactory()
    {
      return traceFactory_;
    }

    @Override
    public T withUnprocessableMessageConsumer(IThreadSafeErrorConsumer<P> unprocessableMessageConsumer)
    {
      unprocessableMessageConsumer_ = unprocessableMessageConsumer;
      
      return self();
    }

    @Override
    public void validate(FaultAccumulator faultAccumulator)
    {
      super.validate(faultAccumulator);
      
      faultAccumulator.checkNotNull(traceFactory_, "traceFactory");
      faultAccumulator.checkNotNull(unprocessableMessageConsumer_, "unprocessableMessageConsumer");
      faultAccumulator.checkNotNull(config_, "config");
    }
  }
  
  protected ICounter getCounter()
  {
    return counter_;
  }
  
  protected abstract void initSubscription(ISubscription<P> subscription);
  protected abstract void startSubscriptions();
  /**
   * Stop all subscribers.
   */
  protected abstract void stopSubscriptions();
  
  protected ITraceContextTransactionFactory getTraceFactory()
  {
    return traceFactory_;
  }

  @Override
  public synchronized void start()
  {
    setLifeCycleState(FugueLifecycleState.Starting);
    
    for(ISubscription<P> s : subscribers_)
    {
      initSubscription(s);
    }
    
    startSubscriptions();
    
    setLifeCycleState(FugueLifecycleState.Running);
  }

  @Override
  public void quiesce()
  {
    setLifeCycleState(FugueLifecycleState.Quiescing);
    
    stopSubscriptions();
    
    setLifeCycleState(FugueLifecycleState.Quiescent);
  }

  @Override
  public synchronized void stop()
  {
    setLifeCycleState(FugueLifecycleState.Stopping);
    
    stopSubscriptions();
    
    setLifeCycleState(FugueLifecycleState.Stopped);
  }

  /**
   * Handle the given message.
   * 
   * @param consumer  The consumer for the message.
   * @param payload   A received message.
   * @param trace     A trace context.
   * @param messageId A unique ID for the message.
   * 
   * @return The number of milliseconds after which a retry should be made, or -1 if the message was
   * processed and no retry is necessary.
   */
  public long handleMessage(IThreadSafeRetryableConsumer<P> consumer, P payload, ITraceContext trace, String messageId)
  {
    try
    {
      consumer.consume(payload, trace);
    }
    catch(RetryableConsumerException e)
    {
      log_.warn("Transient processing failure, will retry (forever)", e);
      return retryMessage(payload, trace, e, messageId, e.getRetryTime(), e.getRetryTimeUnit(), true);
    }
    catch (RuntimeException  e)
    {
      return retryMessage(payload, trace, e, messageId, FAILED_CONSUMER_RETRY_TIME, false);
    }
    catch (FatalConsumerException e)
    {
      log_.error("Unprocessable message, aborted", e);

      trace.trace("MESSAGE_IS_UNPROCESSABLE");
      
      return abortMessage(payload, trace, e);
    }
    
    return MESSAGE_PROCESSED_OK;
  }

  private long retryMessage(P payload, ITraceContext trace, Throwable cause, String messageId,
      Long retryTime, TimeUnit retryTimeUnit, boolean retryForever)
  {
    long delay =retryTime == null || retryTimeUnit == null ? FAILED_CONSUMER_RETRY_TIME : retryTimeUnit.toMillis(retryTime);
    
    return retryMessage(payload, trace, cause, messageId, delay, retryForever);
  }

  private long retryMessage(P payload, ITraceContext trace, Throwable cause, String messageId, long retryTime, boolean retryForever)
  {
    if(!retryForever)
    {
      Integer cnt = failureCache_.getIfPresent(messageId);
      
      if(cnt == null)
      {
        cnt = 1;
        failureCache_.put(messageId, cnt);
      }
      else
      {
        if(cnt >= FAILURE_CNT_LIMIT)
        {
          log_.error("Retryable processing error failed " + cnt + " times, aborting messageId " + messageId);
          trace.trace("MESSAGE_RETRIES_EXCEEDED");
          failureCache_.invalidate(messageId);
          return abortMessage(payload, trace, cause);
        }
        failureCache_.put(messageId, ++cnt);
      }
      log_.warn("Message processing failed " + cnt + " times, will retry", cause);
    }
    
    return retryTime;
  }

  private long abortMessage(P payload, ITraceContext trace, Throwable e)
  {
    try
    {
      unprocessableMessageConsumer_.consume(payload, trace, e.getLocalizedMessage(), e);
      return MESSAGE_PROCESSED_OK;
    }
    catch(RuntimeException e2)
    {
      log_.error("Unprocessable message consumer failed", e);
      return FAILED_DEAD_LETTER_RETRY_TIME;
    }
  }
}
