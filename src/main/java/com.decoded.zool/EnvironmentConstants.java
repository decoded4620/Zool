package com.decoded.zool;

/**
 * Constants used for the Zool Environment with respect the the local machine. This includes how this machine is running
 * e.g. Dev vs Prod, and how this machine can be aware of other machines. Zool is Cloud Agnostic, so you could
 * technically have different cloud hosts announcing to the same Quorum.
 */
public class EnvironmentConstants {
  /**
   * Set this environment variable to your development server ip, e.g. localhost / 127.0.0.1
   */
  public static final String DEV_SERVER_DNS = "DEV_SERVER_DNS";

  /**
   * Set this environment variable within your host to be its public ip e.g. for AWS servers this would be:
   * ec2-XX-XXX-XX-XX.us-east-2.compute.amazonaws.com
   */
  public static final String PROD_SERVER_DNS = "PROD_SERVER_DNS";
}
