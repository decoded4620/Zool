package com.decoded.zool;

/**
 * Constants used for deploying Zool Applications
 */
public class DeployConstants {
  // TODO - remove AWS prefix from these values.
  /**
   * Set this environment variable to your development server ip, e.g. localhost / 127.0.0.1
   */
  public static final String ENV_DEV_PUB_DNS = "AWS_DEV_SERVER_HOME";

  /**
   * Set this environment variable if your a host
   * e.g. ec2-XX-XXX-XX-XX.us-east-2.compute.amazonaws.com
   */
  public static final String ENV_PUB_DNS = "AWS_SERVER_HOME";
}
