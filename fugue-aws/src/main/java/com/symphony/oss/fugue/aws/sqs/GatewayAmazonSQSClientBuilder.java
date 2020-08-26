/*
 * Copyright 2015-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 * 
 * http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.symphony.oss.fugue.aws.sqs;

import javax.annotation.Generated;

import com.amazonaws.annotation.NotThreadSafe;
import com.amazonaws.client.AwsSyncClientParams;
import com.amazonaws.client.builder.AwsSyncClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

/**
 * Fluent builder for {@link com.amazonaws.services.sqs.AmazonSQS}. Use of the builder is preferred over using
 * constructors of the client class.
 * ** CLASS MODIFIED IN ORDER TO BUILD THE MODIFIED SQSCLIENT **
 **/
@NotThreadSafe
@Generated("com.amazonaws:aws-java-sdk-code-generator")
public class GatewayAmazonSQSClientBuilder extends AwsSyncClientBuilder<AmazonSQSClientBuilder, AmazonSQS> {

  //private static final ClientConfigurationFactory CLIENT_CONFIG_FACTORY = new com.amazonaws.services.sqs.AmazonSQSClientConfigurationFactory();
  private String endpoint_;
  /**
   * @return Create new instance of builder with all defaults set.
   */
  public static GatewayAmazonSQSClientBuilder standard() {
      return new GatewayAmazonSQSClientBuilder();
  }

  /**
   * @return Default client using the {@link com.amazonaws.auth.DefaultAWSCredentialsProviderChain} and
   *         {@link com.amazonaws.regions.DefaultAwsRegionProviderChain} chain
   */
  public static AmazonSQS defaultClient() {
      return standard().build();
  }

  private GatewayAmazonSQSClientBuilder() {
      super(null);
  }
  
  /**
   * Sets the endpoint to be used by the client. 
   *
   * @param endpoint Endpoint to use
   * @return This object for method chaining.
   */
  public final GatewayAmazonSQSClientBuilder withEndpoint(String endpoint)
  {
    this.endpoint_ = endpoint;
    return this;
  }

  /**
   * Construct a synchronous implementation of AmazonSQS using the current builder configuration.
   *
   * @param params
   *        Current builder configuration represented as a parameter object.
   * @return Fully configured implementation of AmazonSQS.
   */
  @Override
  protected AmazonSQS build(AwsSyncClientParams params) {
      return endpoint_ == null ? new GatewayAmazonSQSClient(params) : new GatewayAmazonSQSClient(params, endpoint_);
  }
  
  
}