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

package com.symphony.oss.fugue.server.http.ui;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.symphony.oss.fugue.FugueLifecycleState;
import com.symphony.oss.fugue.http.HttpServerBuilder;
import com.symphony.oss.fugue.http.IResourceProvider;
import com.symphony.oss.fugue.http.RandomAuthFilter;
import com.symphony.oss.fugue.http.ui.servlet.Command;
import com.symphony.oss.fugue.http.ui.servlet.CommandServlet;
import com.symphony.oss.fugue.http.ui.servlet.ICommand;
import com.symphony.oss.fugue.http.ui.servlet.ICommandHandler;
import com.symphony.oss.fugue.http.ui.servlet.IUIPanel;
import com.symphony.oss.fugue.http.ui.servlet.StatusServlet;
import com.symphony.oss.fugue.server.http.AbstractFugueHttpServer;

/**
 * The main component for a Fugue process.
 * 
 * @author Bruce Skingle
 *
 */
public class AbstractFugueUiServer<T extends AbstractFugueUiServer<T>> extends AbstractFugueHttpServer<T> implements IFugueUiServer
{
  private static final Logger                        log_              = LoggerFactory.getLogger(AbstractFugueUiServer.class);

  private static final String APP_SERVLET_ROOT = "/app/";

  private final ImmutableList<IResourceProvider> resourceProviders_;
  private final ImmutableList<ICommand>          commands_;
  private StatusServlet statusServlet_;
  private boolean localWebLogin_;
  private final RandomAuthFilter filter_;

  public AbstractFugueUiServer(Class<T> type, AbstractBuilder<?,?> builder)
  {
    super(type, builder);
    
    resourceProviders_   = ImmutableList.copyOf(builder.resourceProviders_);
    commands_            = ImmutableList.copyOf(builder.commands_);
    filter_              = builder.filter;
    
    builder.statusServlet_.withComponentContainer(this);
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
  protected static abstract class AbstractBuilder<T extends AbstractBuilder<T,B>, B extends AbstractFugueUiServer<B>>
  extends AbstractFugueHttpServer.AbstractBuilder<T, B>
  {
    private final List<IResourceProvider>        resourceProviders_   = new ArrayList<>();
    private final List<ICommand>                 commands_            = new ArrayList<>();
    private StatusServlet statusServlet_;
    private boolean localWebLogin_;
    private RandomAuthFilter filter = null;
    
    protected AbstractBuilder(Class<T> type)
    {
      super(type);
      
//      withCommand(APP_SERVLET_ROOT, "shutdown", 
//          EnumSet.of(FugueLifecycleState.Running,
//              FugueLifecycleState.Initializing,
//              FugueLifecycleState.Starting),
//          () -> 
//          {
//            if(started_)
//              stopFugueServer();
//          });
    }

    @Override
    public T withComponents(Object ...components)
    {
      super.withComponents(components);
      
      for(Object o : components)
      {
        if(o instanceof IResourceProvider)
        {
          resourceProviders_.add((IResourceProvider)o);
        }
        if(o instanceof ICommand)
        {
          commands_.add((ICommand)o);
        }
      }
      
      return self();
    }
    
    /**
     * Generate a random one time authentication token and invoke the local browser to connect to the running server.
     * 
     * @return this (Fluent method).
     */
    public T withLocalWebLogin()
    {
      localWebLogin_ = true;
      
      return self();
    }


    /**
     * Add the given UIPanel to this server.
     * 
     * @param panel A UIPanel.
     * 
     * @return this (Fluent method).
     */
    public T withPanel(IUIPanel panel)
    {
      statusServlet_.addPanel(panel);
      
      return self();
    }

    /**
     * Add the given UIPanel to this server and make it the default panel.
     * 
     * @param panel A UIPanel.
     * 
     * @return this (Fluent method).
     */
    public T withDefaultPanel(IUIPanel panel)
    {
      statusServlet_.setDefaultPanel(panel);
      
      return self();
    }
    
    /**
     * Add the given command to this server.
     * 
     * @param path            The servlet path for the command servlet.
     * @param name            The command name for the UI.
     * @param validStates     Lifecycle states from which this command can be invoked.
     * @param handler         The handler which implements the command.
     * 
     * @return this (Fluent method).
     */
    public T withCommand(String path, String name, 
        EnumSet<FugueLifecycleState> validStates,
        ICommandHandler handler)
    {
      path = path + name;
      name = name.substring(0,1).toUpperCase() + name.substring(1);
      
      ICommand command = new Command(name, path, validStates, handler);
      
      register(command);
      
      return self();
    }

    protected void configureServer(HttpServerBuilder httpServerBuilder)
    {
      
      
      if(localWebLogin_)
      {
        filter = new RandomAuthFilter();
        httpServerBuilder.withFilter(filter);
      }
      for(IResourceProvider provider : resourceProviders_)
        httpServerBuilder.withResources(provider);
      
      List<IResourceProvider> resourceProviders = resourceProviders_;
      
      if(!resourceProviders.isEmpty())
      {
        statusServlet_ = new StatusServlet(resourceProviders.get(0));
        httpServerBuilder.withServlet(statusServlet_);
        
        for(ICommand command : commands_)
        {
          httpServerBuilder.withServlet(command.getPath(),  new CommandServlet(command.getHandler()));
          statusServlet_.addCommand(command);
        }
      }
      
      super.configureServer(httpServerBuilder);
    }
  }
  
  protected void startFugueServer()
  {
    super.startFugueServer();
    
    if(filter_ != null)
    {
      openBrowser(RandomAuthFilter.LOGIN_TOKEN + "=" + filter_.getAuthToken());
    }
  }

  
  /**
   * Open the browser on the URL for this server.
   * 
   * @param queryString An optional query string.
   */
  public void openBrowser(String queryString)
  {
    try
    {
      if(getServerUrl() != null)
      {
        String url = getServerUrl() + "/fugue";
        
        if(queryString != null)
          url = url + "?" + queryString;
        
        log_.info("opening browser on " + url);
        
        Runtime.getRuntime().exec("open " + url);
      }
    }
    catch(IOException e)
    {
      log_.error("Failed to open browser", e);
    }
  }

  @Override
  public List<IResourceProvider> getResourceProviders()
  {
    return resourceProviders_;
  }

  @Override
  public List<ICommand> getCommands()
  {
    return commands_;
  }
}
