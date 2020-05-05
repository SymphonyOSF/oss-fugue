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

package com.symphony.oss.fugue.container;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.symphony.oss.commons.fault.CodingFault;
import com.symphony.oss.commons.fluent.BaseAbstractBuilder;
import com.symphony.oss.commons.fluent.IFluent;
import com.symphony.oss.fugue.FugueLifecycleBase;
import com.symphony.oss.fugue.FugueLifecycleState;
import com.symphony.oss.fugue.IFugueComponent;
import com.symphony.oss.fugue.IFugueLifecycleComponent;

/**
 * An abstract fluent container of Fugue components.
 * 
 * @author Bruce Skingle
 *
 * @param <T> The concrete type to be returned by fluent methods.
 */
public abstract class AbstractComponentContainer<T extends AbstractComponentContainer<T>> extends FugueLifecycleBase implements IFugeComponentContainer, IFluent<T>
{
  private static final long                       MEGABYTE             = 1024L * 1024L;

  private static final Logger                     log_                 = LoggerFactory
      .getLogger(AbstractComponentContainer.class);

  private final T self_;
  private final ImmutableList<IFugueComponent>          components_;
  private final ImmutableList<IFugueLifecycleComponent> lifecycleComponents_;

  private ArrayDeque<IFugueComponent>          stopStack_           = new ArrayDeque<>();
  private int                                     maxMemory_;
  private int                                     maxHeapSize_;
  private String                                  pid_;
  private boolean                                 running_;

  
  /**
   * Constructor.
   * 
   * @param type The concrete type returned by fluent methods.
   */
  protected AbstractComponentContainer(Class<T> type, AbstractBuilder<?,?> builder)
  {
    if (!(type.isInstance(this)))
      throw new CodingFault("Class is declared to be " + type + " in type parameter T but it is not.");

    @SuppressWarnings("unchecked")
    T s = (T) this;

    self_ = s;
    components_          = ImmutableList.copyOf(builder.components_);
    lifecycleComponents_ = ImmutableList.copyOf(builder.lifecycleComponents_);
  }

  @Override
  public T self()
  {
    return self_;
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
  protected static abstract class AbstractBuilder<T extends AbstractBuilder<T,B>, B extends AbstractComponentContainer<B>>
  extends BaseAbstractBuilder<T, B>
  implements IFugueComponentRegistry
  {
    private final List<IFugueComponent>          components_          = new ArrayList<>();
    private final List<IFugueLifecycleComponent> lifecycleComponents_ = new ArrayList<>();
    
    protected AbstractBuilder(Class<T> type)
    {
      super(type);
    }

    /**
     * Add each of the given objects as components.
     * 
     * The container is aware of a number of types of component and treats them appropriately.
     * 
     * @param components A varargs list of components.
     * 
     * @return This (fluent method).
     */
    public T withComponents(Object ...components)
    {
      for(Object o : components)
      {
        if(o instanceof IFugueComponent)
        {
          components_.add((IFugueComponent)o);
        }
        if(o instanceof IFugueLifecycleComponent)
        {
          lifecycleComponents_.add((IFugueLifecycleComponent)o);
        }
      }
      
      return self();
    }
    
    @Override
    public <C> C register(C component)
    {
      withComponents(component);
      
      return component;
    }
  }
  
  @Override
  public List<IFugueComponent> getComponents()
  {
    return components_;
  }

  @Override
  public List<IFugueLifecycleComponent> getLifecycleComponents()
  {
    return lifecycleComponents_;
  }
  
  /**
   * Start the container.
   * 
   * Calls start() on all registered components which implement IFugueComponent.
   * 
   * Unless some component starts a non-daemon thread the process will terminate. If no thread
   * exists to keep the application alive then call join() after this method returns since
   * this method is fluent you can call <code>start().join()</code>.
   * 
   * @throws IllegalStateException  If the current state is not compatible with starting.
   * 
   * @return This (fluent method).
   */
  public T start()
  {
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        FugueLifecycleState state = getLifecycleState();
        
        System.err.println("Shutdown hook called from state " + state);
        
        switch(state)
        {
          case Initializing:
          case Running:
          case Starting:
            System.err.println("Attempting to quiesce...");
            quiesce();
            // fall through
            
          case Quiescing:
          case Quiescent:
            System.err.println("Attempting clean shutdown...");
            setLifeCycleState(FugueLifecycleState.Stopping);
            
            if(doStop())
            {
              System.err.println("Faild to stop cleanly");
            }
            else
            {
              setLifeCycleState(FugueLifecycleState.Stopped);
              System.err.println("Attempting clean shutdown...DONE");
            }
            break;
            
          case Stopped:
            break;
              
          default:
            try
            {
              // Sleep for 5 seconds in the hope that threads have time to finish....
              System.err.println("Sleep for 5 seconds...");
              Thread.sleep(5000);
            }
            catch (InterruptedException e)
            {
              System.err.println("Sleep for 5 seconds...INTERRUPTED");
              e.printStackTrace();
            }
            System.err.println("Sleep for 5 seconds...DONE");
        }
      }
    }));
    
    setLifeCycleState(FugueLifecycleState.Starting);
    
    for(IFugueComponent component : components_)
    {
      try
      {
        
        log_.debug("Start " + component);
        component.start(); 
        stopStack_.push(component);
      }
      catch(RuntimeException ex)
      {
        log_.error("Unable to start component " + 
            component, ex);
        
        setLifeCycleState(FugueLifecycleState.Failed);
        
        doStop();
        
        log_.error("Faild to start cleanly : CALLING System.exit()");
        System.exit(1);
      }
    }
    setLifeCycleState(FugueLifecycleState.Running);
    setRunning(true);
    return self();
  }
  
  /**
   * Stop the container.
   * 
   * Calls quiesce() on all registered components which implement IFugueComponent.
   * 
   * @return This (fluent method).
   */
  public T quiesce()
  {
    RuntimeException error = null;
    
    setLifeCycleState(FugueLifecycleState.Quiescing);
    
    log_.info("Quiescing...");
    
    for(IFugueComponent component : stopStack_)
    {
      try
      {
        log_.debug("Quiesce " + component);
        component.quiesce();
      }
      catch(RuntimeException ex)
      {
        log_.error("Unable to quiesce component " + 
            component, ex);
        // Don't re-throw because we want other components to have a chance to stop
        
        error = ex;
      }
    }
    
    if(error == null)
    {
      setLifeCycleState(FugueLifecycleState.Quiescent);
    }
    else
    {
      setLifeCycleState(FugueLifecycleState.Failed);
      throw error;
    }
    return self();
  }

  /**
   * Stop the container.
   * 
   * Calls stop() on all registered components which implement IFugueComponent.
   * 
   * @return This (fluent method).
   */
  public T stop()
  {
    setLifeCycleState(FugueLifecycleState.Stopping);
    
    if(doStop())
    {
      log_.error("Faild to stop cleanly : CALLING System.exit()");
      System.exit(1);
    }
    setLifeCycleState(FugueLifecycleState.Stopped);
    
    return self();
  }
  
  private boolean doStop()
  {
    boolean terminate = false;
    
    log_.info("Stopping...");
    
    while(!stopStack_.isEmpty())
    {
      IFugueComponent component = stopStack_.pop();
      try
      {
        log_.debug("Stop " + component);
        component.stop();
      }
      catch(RuntimeException ex)
      {
        log_.error("Unable to stop component " + 
            component, ex);
        // Don't re-throw because we want other components to have a chance to stop
        
        terminate = true;
        setLifeCycleState(FugueLifecycleState.Failed);
      }
    }
    
    return terminate;
  }

  
  @Override
  public synchronized boolean isRunning()
  {
    return running_;
  }

  @Override
  public synchronized boolean setRunning(boolean running)
  {
    boolean v = running_;
    running_ = running;
    
    if(!running)
      notifyAll();
    
    return v;
  }
  
  /**
   * Run the main loop process for up to timeout milliseconds.
   * 
   * This is an alternative to calling join() on a server object from the main thread.
   * This method periodically prints various debug information to the log.
   * 
   * @param timeout Limit in ms of time to run.
   * @return this (Fluent method).
   * 
   * @throws InterruptedException If a sleep is interrupted.
   */
  public T mainLoop(long timeout) throws InterruptedException
  {
    long endTime = timeout <= 0 ? Long.MAX_VALUE : System.currentTimeMillis() + timeout;
    Runtime runtime = Runtime.getRuntime();
    pid_ = getPid();
    
    File statm = new File("/proc/self/statm");
    
//    ProcessBuilder builder = new ProcessBuilder()
//        .command("ps", "-o", "pid,rss,vsz,time");
    
    while(isRunning() && System.currentTimeMillis() < endTime)
    {
      int heapSize = (int) ((runtime.totalMemory() - runtime.freeMemory()) / MEGABYTE);
      
      if(heapSize > maxHeapSize_)
        maxHeapSize_ = heapSize;
      
      log_.info(String.format("JVM Memory: used = %4d, free = %4d, total = %4d, max = %4d, %3d processors", 
          heapSize,
          runtime.freeMemory() / MEGABYTE, runtime.totalMemory() / MEGABYTE, runtime.maxMemory() / MEGABYTE,
          runtime.availableProcessors()));
      
      if(statm.exists())
      {
        readMem(statm);

        log_.info("pid " + pid_ + " maxMemory = " + maxMemory_ + ", maxHeap = " + maxHeapSize_);
      }
      
//      run(builder);
      
      long bedtime = endTime - System.currentTimeMillis();
      
      if(bedtime > 60000)
        bedtime = 60000;
      
      if(bedtime>0)
      {
        synchronized(this)
        {
          wait(bedtime);
        }
      }
    }
    
    return self();
  }

//  private void run(ProcessBuilder builder)
//  {
//    try
//    {
//      Process process = builder.start();
//      
//      try(BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream())))
//      {
//        String line = in.readLine();
//        log_.info(line);
//        while((line = in.readLine()) != null)
//        {
//          String[] words = line.trim().split(" +");
//          
//          if(pid_.equals(words[0]))
//          {
//            log_.info(line);
//            try
//            {
//              String word = words[1];
//              int mem = 0;
//              
//              if(word.endsWith("m"))
//                mem = Integer.parseInt(word.substring(0, word.length()-1));
//              else if(word.endsWith("g"))
//                  mem = (int)(1000 * Double.parseDouble(word.substring(0, word.length()-1)));
//              else
//                mem = Integer.parseInt(word);
//              
//              if(mem > maxMemory_)
//                maxMemory_ = mem;
//            }
//            catch(NumberFormatException e)
//            {
//              log_.error("Failed to parse memory", e);
//            }
//          }
//        }
//      }
//      
//      try(BufferedReader in = new BufferedReader(new InputStreamReader(process.getErrorStream())))
//      {
//        String line;
//        while((line = in.readLine()) != null)
//          log_.warn(line);
//      }
//    }
//    catch (IOException e)
//    {
//      log_.error("Unable to run command", e);
//    }
//  }

  private void readMem(File statm)
  {
    try(InputStream in = new FileInputStream(statm))
    {
      StringBuilder b = new StringBuilder();
      int           c;
      
      while((c = in.read()) != -1)
      {
        b.append((char)c);
      }
      
      log_.info("Mem: " + b);
    }
    catch (IOException e)
    {
      log_.warn("Unable to read memory usage", e);
    }
  }

  private String getPid()
  {
    String processName =
        java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
      
    return processName.split("@")[0];
  }
}
