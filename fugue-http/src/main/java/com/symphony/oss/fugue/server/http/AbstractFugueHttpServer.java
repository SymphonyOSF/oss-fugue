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

package com.symphony.oss.fugue.server.http;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.symphony.oss.commons.fault.FaultAccumulator;
import com.symphony.oss.fugue.FugueComponentState;
import com.symphony.oss.fugue.FugueLifecycleState;
import com.symphony.oss.fugue.IFugueApplication;
import com.symphony.oss.fugue.IFugueLifecycleComponent;
import com.symphony.oss.fugue.server.AbstractFugueServer;

public class AbstractFugueHttpServer<T extends AbstractFugueHttpServer<T>> extends AbstractFugueServer<T> implements IFugeHttpServer
{
  private static final Logger                   log_             = LoggerFactory.getLogger(AbstractFugueHttpServer.class);
  
  private String                                serverUrl_;
  private HttpServer                            server_;
  // private StatusServlet statusServlet_;

  private final ImmutableList<IServletProvider>            servletProviders_;
  private final ImmutableList<IUrlPathServlet>             servlets_;

  public AbstractFugueHttpServer(Class<T> type, AbstractBuilder<?, ?> builder)
  {
    super(type, builder);
    
    builder.lifecycleComponent_.server_ = this;

    servletProviders_    = ImmutableList.copyOf(builder.servletProviders_);
    servlets_            = ImmutableList.copyOf(builder.servlets_);

    server_ = builder.httpServerBuilder_.build();
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
  protected static abstract class AbstractBuilder<T extends AbstractBuilder<T,B>, B extends AbstractFugueHttpServer<B>>
  extends AbstractFugueServer.AbstractBuilder<T, B>
  {
    private int                          httpPort_         = 80;
    private final List<IServletProvider> servletProviders_ = new ArrayList<>();
    private final List<IUrlPathServlet>  servlets_         = new ArrayList<>();
    private final LifecycleComponent     lifecycleComponent_;
    private HttpServerBuilder            httpServerBuilder_;
    
    protected AbstractBuilder(Class<T> type)
    {
      super(type);
      
      lifecycleComponent_ = new LifecycleComponent();
            
      register(lifecycleComponent_);
      
      
      register(new HealthCheckServlet());
    }

    @Override
    public T withComponents(Object ...components)
    {
      super.withComponents(components);
      
      for(Object o : components)
      {
        if(o instanceof IServletProvider)
        {
          servletProviders_.add((IServletProvider)o);
        }
        if(o instanceof IUrlPathServlet)
        {
          servlets_.add((IUrlPathServlet)o);
        }
      }
      
      return self();
    }
    
    public T withHttpPort(int httpPort)
    {
      httpPort_ = httpPort;
      
      return self();
    }
    
    public T withFugueApplication(IFugueApplication application)
    {
      httpPort_         = application.getHttpPort();
      
      return withApplicationName(application.getName());
    }
    
    @Override
    protected void validate(FaultAccumulator faultAccumulator)
    {
      super.validate(faultAccumulator);
      
      httpServerBuilder_ = new HttpServerBuilder();
      
      configureServer(httpServerBuilder_);
    }

    protected void configureServer(HttpServerBuilder httpServerBuilder)
    {
      httpServerBuilder.withHttpPort(httpPort_);

    
      for(IServletProvider servletProvider : servletProviders_)
      {
        servletProvider.registerServlets(httpServerBuilder);
      }
      
      for(IUrlPathServlet servlet : servlets_)
      {
        httpServerBuilder.withServlet(servlet);
      }
    }
  }

  static class LifecycleComponent implements IFugueLifecycleComponent
  {
    public AbstractFugueHttpServer<?> server_;

    @Override
    public void start()
    {
  //    startFugueServer();
    }
    
    @Override
    public void quiesce()
    {
    }
  
    @Override
    public void stop()
    {
      server_.stopFugueServer();
    }
  
    @Override
    public FugueLifecycleState getLifecycleState()
    {
      return server_.getLifecycleState();
    }
  
    @Override
    public FugueComponentState getComponentState()
    {
      return server_.getComponentState();
    }
  
    @Override
    public String getComponentStatusMessage()
    {
      return server_.getComponentStatusMessage();
    }
  }
  
  protected void startFugueServer()
  {
    super.startFugueServer();
    
    try
    {
      server_.start();
      
      int port = server_.getLocalPort();
      
      serverUrl_ = "http://127.0.0.1:" + port;
      
      log_.info("server started on " + serverUrl_);
      log_.info("you can also point your browser to http://" + 
          InetAddress.getLocalHost().getHostName() + ":" + port);
      log_.info("you can also point your browser to http://" + 
          InetAddress.getLocalHost().getHostAddress() + ":" + port);

      
    }
    catch(IOException e)
    {
      setLifeCycleState(FugueLifecycleState.Failed);
      setComponentState(FugueComponentState.Failed);
      setStatusMessage(e.toString());
      log_.error("Start failed", e);
    }
  }

  protected void stopFugueServer()
  {
    server_.stop();
    log_.info("FugueServer Stopping...");
    
    super.stopFugueServer();
  }

  @Override
  public int getHttpPort()
  {
    return server_.getLocalPort();
  }

  @Override
  public String getServerUrl()
  {
    return serverUrl_;
  }

  @Override
  public List<IServletProvider> getServletProviders()
  {
    return servletProviders_;
  }

  @Override
  public List<IUrlPathServlet> getServlets()
  {
    return servlets_;
  }
}
