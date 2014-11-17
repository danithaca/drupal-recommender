Drupal Recommender API: Java Agent Program
==========================================

This documentation helps you setup and run the Java agent program of [Recommender API](http://drupal.org/project/recommender) Drupal module. Compares to the PHP recommender engine that comes with Recommender API, this Java agent program offers these additional features:

  * Apache Mahout integration for better performance and scalability.
  * Deployment on a remote server instead of on the Drupal production server.
  * Additional config parameters (e.g., incremental update) for the classical collaborative filtering algorithm (user-based and item-based).
  * Advanced recommender algorithms such as SVD++ and ensemble (to be implemented).
  * Hadoop/Spark clusters support for big data (to be implemented).
  
Current release: _7.x-6.0-alpha1_ (tag) in the _7.x-6.x_ branch, following the release convention of the [Recommender API](http://drupal.org/project/recommender) Drupal module.


Installation
------------

This program requires the [Drupal Computing module](http://drupal.org/project/computing). Please refer to https://github.com/danithaca/drupal-computing and set up Drupal Computing Java client library on the server where you will run the Recommender API Java agent program. Then download this program to the same server.

Additional Java library dependencies:

  * [Apache Mahout](http://mahout.apache.org/): tested on v0.9.
  * [Apache Commons DBCP](http://commons.apache.org/dbcp/): tested on v1.4
  * [Apache Commons DbUtils](http://commons.apache.org/dbutils/): tested on v1.6
  * JDBC drivers, such as [MySQL JDBC Connector](http://dev.mysql.com/downloads/connector/j/)

Maven is not yet supported for dependency management (follow the issue at https://github.com/danithaca/drupal-recommender/issues/1). Here are a few links to help you use Maven with the Recommender API Java agent:

  * http://stackoverflow.com/questions/364114/can-i-add-jars-to-maven-2-build-classpath-without-installing-them
  * http://stackoverflow.com/questions/2472376/how-do-i-execute-a-program-using-maven


Configuration and Execution
---------------------------

Configuring the Java agent mostly involves setup the correct CLASSPATH. For example:

    export MAHOUT_HOME=/opt/mahout-distribution-0.9
    export COMMONS_DBUTILS_HOME=/opt/commons-dbutils-1.6
    export COMMONS_DBCP_HOME=/opt/commons-dbcp-1.4
    export MYSQL_JDBC_HOME=/opt/mysql-connector
    export DRUPAL_COMPUTING_HOME=/opt/drupal-computing
    
    export CLASSPATH=${CLASSPATH}:recommender.jar:${MAHOUT_HOME}/*:${COMMONS_DBUTILS_HOME}/*:${COMMONS_DBCP_HOME}/*:${MYSQL_JDBC_HOME}/*:${DRUPAL_COMPUTING_HOME}/java/computing.jar:${DRUPAL_COMPUTING_HOME}/java/lib/*

Then, fire up the class that executes Recommender, which takes input from the Recommender API Drupal module:

    java -Ddcomp.config.file=config.properties org.drupal.project.recommender.DefaultApplication
    
Please refer to the [Recommender API](http://drupal.org/project/recommender) Drupal module to learn more about the parameters for recommender algorithms. You also need to specify additional parameters in _config.properties_ file about how the Java agent interact with Drupal. See documentations at https://github.com/danithaca/drupal-computing.


Development
-----------

Read the Java source code directly to see how to extends the Recommender API Java agent. To develop sub-modules for Recommender API, please refer to documentations on http://drupal.org/project/recommender.