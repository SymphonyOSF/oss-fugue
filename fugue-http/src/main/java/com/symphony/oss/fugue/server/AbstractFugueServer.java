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

package com.symphony.oss.fugue.server;

import java.net.URL;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.symphony.oss.commons.concurrent.NamedThreadFactory;
import com.symphony.oss.commons.fault.FaultAccumulator;
import com.symphony.oss.fugue.FugueComponentState;
import com.symphony.oss.fugue.FugueLifecycleState;
import com.symphony.oss.fugue.container.AbstractComponentContainer;
import com.symphony.oss.fugue.server.http.HealthCheckServlet;

/**
 * The main component for a Fugue process.
 * 
 * @author Bruce Skingle
 *
 */
public class AbstractFugueServer<T extends AbstractFugueServer<T>> extends AbstractComponentContainer<T> implements IFugueServer
{
  private static final Logger                   log_             = LoggerFactory.getLogger(AbstractFugueServer.class);

  private static final String                   APP_SERVLET_ROOT = "/app/";

  private final String                          applicationName_;

  private CopyOnWriteArrayList<ExecutorService> executors_       = new CopyOnWriteArrayList<>();
  private CopyOnWriteArrayList<Thread>          threads_         = new CopyOnWriteArrayList<>();

  // private IApplication application_;
  private boolean                               started_;
  private FugueComponentState                   componentState_  = FugueComponentState.OK;
  private String                                statusMessage_   = "Initializing...";


  
  protected AbstractFugueServer(Class<T> type, AbstractBuilder<?, ?> builder)
  {
    super(type, builder);
    
    applicationName_  = builder.applicationName_;
  }

  /**
   * The builder implementation.
   * 
   * Any sub-class of FugueLifecycleComponent would need to implement its own Abstract sub-class of this class
   * and then a concrete Builder class which is itself a sub-class of that.
   * 
   * @author Bruce Skingle
   *
   * @param <T> The type of the concrete Builder
   * @param <B> The type of the built class, some subclass of FugueLifecycleComponent
   */
  protected static abstract class AbstractBuilder<T extends AbstractBuilder<T,B>, B extends AbstractFugueServer<B>>
  extends AbstractComponentContainer.AbstractBuilder<T, B>
  {
    private String                               applicationName_ = "Application";
    
    protected AbstractBuilder(Class<T> type)
    {
      super(type);
      
      String configFile = System.getProperty("log4j.configurationFile");
      
      if(configFile == null)
      {
        URL url = getClass().getClassLoader().getResource("log4j2.xml");
        System.out.println("log4j2.xml from resources = " + url);
      }
      else
      {
        System.out.println("log4j2.xml from system property = " + configFile);
      }
      
      //register(new LifecycleComponent());
    }

    public T withApplicationName(String applicationName)
    {
      applicationName_ = applicationName;
      
      return self();
    }

    @Override
    protected void validate(FaultAccumulator faultAccumulator)
    {
      super.validate(faultAccumulator);
    }
  }
  

  public FugueComponentState getComponentState()
  {
    return componentState_;
  }

  public String getComponentStatusMessage()
  {
    return statusMessage_;
  }

  @Override
  public String getApplicationName()
  {
    return applicationName_;
  }

  @Override
  public T start()
  {
    log_.info("Start FugueServer");
    super.start();
    
    startFugueServer();
    
    return self();
  }

  /**
   * Join the calling thread to the server process.
   * 
   * This method will block until the server exits. This is useful if the main thread of the process has nothing
   * to do apart from to start the server.
   * 
   * @return this (fluent method)
   * 
   * @throws InterruptedException If the thread is interrupted.
   */
  public synchronized T join() throws InterruptedException
  {
    return mainLoop(0L);
//    while(running_)
//      wait();
//    
//    return this;
  }

  /**
   * Put the server into a failed state.
   * 
   * @return this (fluent method) 
   */
  public T fail()
  {
    log_.error("Server FAILED");
    return stop();
  }

  /**
   * Add the current thread to the list of managed threads.
   * 
   * Managed threads are interrupted when the server shuts down.
   * 
   * @return this (Fluent method).
   */
  public T withCurrentThread()
  {
    return withThread(Thread.currentThread());
  }
  
 
  /**
   * Add the given thread to the list of managed threads.
   * 
   * Managed threads are interrupted when the server shuts down.
   * 
   * @param thread The thread to be managed.
   * 
   * @return this (Fluent method).
   */
  public T withThread(Thread thread)
  {
    threads_.add(thread);
    
    return self();
  }

  protected void setComponentState(FugueComponentState componentState)
  {
    componentState_ = componentState;
  }

  protected void setStatusMessage(String statusMessage)
  {
    statusMessage_ = statusMessage;
  }

  protected void startFugueServer()
  {
    if(started_)
      return;
    
    started_ = true;
    
    log_.info("FugueServer Started");
    
    setLifeCycleState(FugueLifecycleState.Running);
    statusMessage_ = "";  
  }

  protected void stopFugueServer()
  {
    setRunning(false);
    
    if(!started_)
    {
      log_.info("Not started, no need to stop");
      return;
    }

    setLifeCycleState(FugueLifecycleState.Stopping);
    statusMessage_ = "Shutting down...";
    
    for(ExecutorService exec : executors_)
      exec.shutdown();
    
    for(Thread thread : threads_)
      thread.interrupt();
    
    waitForAllExecutors(5000);
    
    for(ExecutorService exec : executors_)
    {
      if(!exec.isTerminated())
      {
        log_.warn("Executor " + exec + " did not terminate cleanly, calling shutdownNow...");
        exec.shutdownNow();
      }
    }
    
    for(Thread thread : threads_)
    {
      if(thread.isAlive())
        log_.error("Thread " + thread + " did not terminate cleanly");
    }
    
    waitForAllExecutors(5000);
    
    for(ExecutorService exec : executors_)
    {
      if(!exec.isTerminated())
      {
        log_.error("Executor " + exec + " did not terminate cleanly");
      }
    }
    
    started_ = false;

    setLifeCycleState(FugueLifecycleState.Stopped);
    statusMessage_ = "Stopped cleanly.";

    log_.info("FugueServer Stopped cleanly");
    
    /* The kill thread is a daemon thread which sleeps for 5 minutes and then calls System.exit. If
     * everything is working it will never take effect because at this point all non-daemon threads should
     * have terminated.
     * 
     * This is a fail safe to ensure that if any thread is left running in error that the JVM will eventually
     * terminate, but if this happens it is an ERROR as all threads should exit cleanly.
     */
    startKillThread();
    
    log_.info("Started daemon killThread, let's hope it does not take effect...");
  }
  
  private void startKillThread()
  {
    Thread killThread = new Thread()
    {

      @Override
      public void run()
      {
        try
        {
          Thread.sleep(300000);
        }
        catch (InterruptedException e)
        {
          log_.error("Kill thread was interrupted", e);
        }
        
        log_.error("Kill thread is still alive, calling System.exit()");
        System.exit(1);
        log_.error("OMG!!! Kill thread is still alive AFTER calling System.exit()!!!!");
      }
  
    };
    
    killThread.setDaemon(true);
    killThread.start();
  }

  private void waitForAllExecutors(int delayMillis)
  {
    long timeout = System.currentTimeMillis() + delayMillis;
    
    log_.info("Waiting up to " + delayMillis + "ms for all executors...");
    
    for(ExecutorService exec : executors_)
    {
      long wait = timeout - System.currentTimeMillis();
      
      if(wait > 0)
      {
        try
        {
          exec.awaitTermination(wait, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e)
        {
          log_.info("Interrupted waiting for executor termination");
        }
      }
    }
    
    log_.info("Waiting up to " + delayMillis + "ms for all executors...Done");
  }
  
  @Override
  public ExecutorService newExecutor(String name)
  {
    ExecutorService fugueExec = new ThreadPoolExecutor(5, 20,
        500L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new NamedThreadFactory(name));
    
    executors_.add(fugueExec);
    
    return fugueExec;
  }
  
  @Override
  public ExecutorService newExecutor(ExecutorService exec)
  {
    executors_.add(exec);
    
    return exec;
  }
  
  @Override
  public ScheduledExecutorService newScheduledExecutor(String name)
  {
    ScheduledExecutorService exec =  Executors.newScheduledThreadPool(0, new NamedThreadFactory(name));
    
    executors_.add(exec);
    
    return exec;
  }
  
  @Override
  public ScheduledExecutorService newScheduledExecutor(ScheduledExecutorService exec)
  {
    executors_.add(exec);
    
    return exec;
  }
}
