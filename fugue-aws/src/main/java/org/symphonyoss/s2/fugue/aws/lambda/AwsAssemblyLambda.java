/*
 * Copyright 2018 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package org.symphonyoss.s2.fugue.aws.lambda;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.symphony.oss.fugue.IFugueAssemblyBuilder;
import com.symphony.oss.fugue.config.IConfiguration;
import com.symphony.oss.fugue.counter.BusyCounter;
import com.symphony.oss.fugue.counter.IBusyCounter;
import com.symphony.oss.fugue.counter.ITopicBusyCounterFactory;
import com.symphony.oss.fugue.server.FugueComponentContainer;

/**
 * A lambda function implementation based on a Fugue Assembly.
 * 
 * @author Bruce Skingle
 *
 */
public abstract class AwsAssemblyLambda implements RequestStreamHandler
{
  private static final Logger log_ = LoggerFactory.getLogger(AwsAssemblyLambda.class);

  protected abstract IFugueAssemblyBuilder<?,?> createBuilder();

  @Override
  public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)
  {
    try
    {
      FugueComponentContainer.Builder registry = new FugueComponentContainer.Builder();
      
      IFugueAssemblyBuilder<?, ?> builder = createBuilder();
      
      LambdaBusyCounterFactory busyCounterFactory =  new LambdaBusyCounterFactory(
          builder.getConfiguration().getConfiguration("com/symphony/s2/legacy/message/forwarder/AwsForwarderComponent"));
      
      builder
          .withContainer(registry)
          .withBusyCounterFactory(busyCounterFactory) // TODO: refactor this into Symphony code
          .build();
            
      FugueComponentContainer container = registry.build();
      
      busyCounterFactory.container_ = container;
      
      //container.start();
      
      try
      {
        int timeout = context.getRemainingTimeInMillis() - 60000;
        
        log_.info("context.getRemainingTimeInMillis()=" + context.getRemainingTimeInMillis() + ", timeout=" + timeout);
        
        container.mainLoop(timeout);
      }
      finally
      {
        log_.info("Quiescing...");
        container.quiesce();
        log_.info("Stopping...");
        container.stop();
        log_.info("Stopping...Done.");
      }
      
      new AwsLambdaResponse(200, "OK").write(outputStream);
      
      
    }
    catch(IOException | InterruptedException e)
    {
      log_.error("Failed to process", e);
      try
      {
        new AwsLambdaResponse(500, e.toString()).write(outputStream);
      }
      catch (IOException e1)
      {
        log_.error("Failed to write error", e1);
      }
    }
  }
  
  class LambdaBusyCounterFactory implements ITopicBusyCounterFactory
  {
    private FugueComponentContainer container_;
    private final IConfiguration config_;

    private LambdaBusyCounterFactory(IConfiguration config)
    {
      config_ = config;
    }

    @Override
    public IBusyCounter create(String topicId)
    {
      return new LambdaBusyCounter(container_, config_);
    }
    
  }
  
  class LambdaBusyCounter extends BusyCounter
  {
    private FugueComponentContainer container_;

    private LambdaBusyCounter(FugueComponentContainer container, IConfiguration config)
    {
      super(config);
      container_ = container;
    }

    @Override
    protected boolean scaleDown()
    {
      log_.info("Received SCALE DOWN so terminating main loop...");
      container_.setRunning(false);
      
      return true;
    }
  }
}
