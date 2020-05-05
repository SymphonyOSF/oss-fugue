/*
 * Copyright 2018 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package com.symphony.oss.fugue.google.pubsub;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.symphony.oss.commons.fault.FaultAccumulator;
import com.symphony.oss.fugue.config.IConfiguration;
import com.symphony.oss.fugue.naming.INameFactory;
import com.symphony.oss.fugue.naming.Name;
import com.symphony.oss.fugue.pipeline.IThreadSafeErrorConsumer;
import com.symphony.oss.fugue.pubsub.AbstractSubscriberManager;
import com.symphony.oss.fugue.pubsub.ISubscription;
import com.symphony.oss.fugue.trace.ITraceContextTransactionFactory;

import io.grpc.StatusRuntimeException;

/**
 * Subscriber manager for Google pubsub.
 * 
 * The following configuration is supported:
 * 
  "org":
  {
    "symphonyoss":
    {
      "s2":
      {
        "fugue":
        {
          "google":
          {
            "pubsub":
            {
              "subscriberThreadPoolSize": 40
            }
          }
        }
      }
    }
  }
 * @author Bruce Skingle
 *
 */
public class GoogleAsyncSubscriberManager extends AbstractSubscriberManager<String, GoogleAsyncSubscriberManager>
{
  private static final Logger log_            = LoggerFactory.getLogger(GoogleAsyncSubscriberManager.class);

  private final String        projectId_;

  private List<Subscriber>    subscriberList_ = new LinkedList<>();
  List<GoogleAsyncSubscriber> receiverList_   = new LinkedList<>();
  private int                 subscriptionErrorCnt_;
//  private final IConfiguration pubSubConfig_;
//
//  private ICounter counter_;


  private GoogleAsyncSubscriberManager(Builder builder)
  {
    super(builder);
    
    projectId_ = builder.projectId_;
  }
  
  /**
   * Concrete builder.
   * 
   * @author Bruce Skingle
   *
   */
  public static class Builder extends AbstractSubscriberManager.Builder<Builder, String, GoogleAsyncSubscriberManager>
  {
    private String                 projectId_;

    /**
     * Constructor.
     * 
     * @param nameFactory                     A NameFactory.
     * @param traceFactory                    A trace context factory.
     * @param unprocessableMessageConsumer    Consumer for invalid messages.
     * @param config                          Configuration
     * @param projectId                       The Google project ID for the pubsub service.
     */
    public Builder(INameFactory nameFactory, ITraceContextTransactionFactory traceFactory,
        IThreadSafeErrorConsumer<String> unprocessableMessageConsumer, IConfiguration config, String projectId)
    {
      super(Builder.class);
    }
    
    /**
     * Set the Google project ID.
     * 
     * @param projectId The ID of the Google project in which to operate.
     * 
     * @return this (fluent method)
     */
    public Builder withProjectId(String projectId)
    {
      projectId_  = projectId;
      
      return self();
    }

    @Override
    public void validate(FaultAccumulator faultAccumulator)
    {
      super.validate(faultAccumulator);
      
      faultAccumulator.checkNotNull(projectId_, "projectId");
    }

    @Override
    protected GoogleAsyncSubscriberManager construct()
    {
      return new GoogleAsyncSubscriberManager(this);
    }
  }

  @Override
  protected void initSubscription(ISubscription<String> subscription)
  { 
    for(Name subscriptionName : subscription.getSubscriptionNames())
    {
      log_.info("Validating subscription " + subscriptionName + "...");
      
      validateSubcription(subscriptionName.toString());
      
    }
    
    if(subscriptionErrorCnt_>0)
    {
      throw new IllegalStateException("There are " + subscriptionErrorCnt_ + " subscription errors.");
    }
    
    long threadsPerSubscription = config_.getLong("subscriberThreadsPerSubscription", 10L);
    int subscriberThreadPoolSize = config_.getInt("subscriberThreadPoolSize", 4);
    long maxOutstandingElementCount = config_.getLong("maxOutstandingElementCount", threadsPerSubscription);
    
    threadsPerSubscription = 1L;
    subscriberThreadPoolSize = 1;
    maxOutstandingElementCount = 1L;
    
    log_.info("Starting subscriptions threadsPerSubscription=" + threadsPerSubscription +
        " subscriberThreadPoolSize=" + subscriberThreadPoolSize +
        " maxOutstandingElementCount=" + maxOutstandingElementCount +
        " ...");

    
    for(Name subscriptionName : subscription.getSubscriptionNames())
    {
      log_.info("Subscribing to " + subscriptionName + " ...");
      
      GoogleAsyncSubscriber   receiver                = new GoogleAsyncSubscriber(this, getTraceFactory(), subscription.getConsumer(), subscriptionName.toString(), counter_, nameFactory_.getPodName());
      ProjectSubscriptionName projectSubscriptionName = ProjectSubscriptionName.of(projectId_, subscriptionName.toString());      
      Subscriber.Builder      builder = Subscriber.newBuilder(projectSubscriptionName, receiver);
      
//        ExecutorProvider executorProvider =
//            InstantiatingExecutorProvider.newBuilder()
//              .setExecutorThreadCount(subscriberThreadPoolSize)
//              .build();
//        builder.setExecutorProvider(executorProvider);
//        
//        
//        
//        builder.setFlowControlSettings(FlowControlSettings.newBuilder()
//            .setMaxOutstandingElementCount(maxOutstandingElementCount).build());
      Subscriber              subscriber              = builder.build();
      
      subscriber.addListener(new Subscriber.Listener()
      {
        @Override
        public void failed(Subscriber.State from, Throwable failure)
        {
          log_.error("Error for " + projectSubscriptionName + " from " + from, failure);
        }
      }, MoreExecutors.directExecutor());
      
      synchronized (subscriberList_)
      {
        subscriberList_.add(subscriber);
        receiverList_.add(receiver);
      }
      
      
    }
  }
  
  @Override
  protected void startSubscriptions()
  {
    synchronized (subscriberList_)
    {
      for(Subscriber subscriber : subscriberList_)
      {
        subscriber.startAsync();
        log_.info("Subscribing to " + subscriber.getSubscriptionNameString() + "...");
      }
    }
  }

  private void validateSubcription(String subscriptionName)
  {
    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create())
    {
      ProjectSubscriptionName projectSubscriptionName = ProjectSubscriptionName.of(projectId_, subscriptionName);
      
      try
      {
        com.google.pubsub.v1.Subscription existing = subscriptionAdminClient.getSubscription(projectSubscriptionName);
        
        log_.info("Subscription " + subscriptionName + " exists with ack deadline " + existing.getAckDeadlineSeconds() + " seconds.");
        
        
      }
      catch(NotFoundException e)
      {   
        log_.error("Subscription " + subscriptionName + " DOES NOT EXIST.");
        subscriptionErrorCnt_++;
      }
      catch(StatusRuntimeException e)
      {
        log_.error("Subscription " + subscriptionName + " cannot be validated - lets hope....", e);
      }
    }
    catch (IOException e)
    {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected void stopSubscriptions()
  {
    synchronized (subscriberList_)
    {
      for(Subscriber subscriber : subscriberList_)
      {
        try
        {
          log_.info("Stopping subscriber " + subscriber.getSubscriptionNameString() + "...");
          
          subscriber.stopAsync().awaitTerminated();
          
          log_.info("Stopped subscriber " + subscriber.getSubscriptionNameString());
        }
        catch(RuntimeException e)
        {
          log_.error("Failed to stop subscriber " + subscriber.getSubscriptionNameString(), e);
        }
      }
    }
  }
}
