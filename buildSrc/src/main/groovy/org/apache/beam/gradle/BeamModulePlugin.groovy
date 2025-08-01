/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.gradle

import static java.util.UUID.randomUUID

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.util.logging.Logger
import org.gradle.api.attributes.Category
import org.gradle.api.GradleException
import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.file.FileCollection
import org.gradle.api.file.FileTree
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.TaskProvider
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.CompileOptions
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.PathSensitivity
import org.gradle.testing.jacoco.tasks.JacocoReport


/**
 * This plugin adds methods to configure a module with Beam's defaults, called "natures".
 *
 * <p>The natures available:
 *
 * <ul>
 *   <li>Java   - Configures plugins commonly found in Java projects
 *   <li>Go     - Configures plugins commonly found in Go projects
 *   <li>Docker - Configures plugins commonly used to build Docker containers
 *   <li>Grpc   - Configures plugins commonly used to generate source from protos
 *   <li>Avro   - Configures plugins commonly used to generate source from Avro specifications
 * </ul>
 *
 * <p>For example, see applyJavaNature.
 */
class BeamModulePlugin implements Plugin<Project> {

  static final Logger logger = Logger.getLogger(BeamModulePlugin.class.getName())

  /** Licence header enforced by spotless */
  static final String javaLicenseHeader = """/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"""
  static def getRandomPort() {
    new ServerSocket(0).withCloseable { socket ->
      def port = socket.getLocalPort()
      if (port > 0) {
        return port
      } else {
        throw new GradleException("couldn't find a free port.")
      }
    }
  }

  /** A class defining the set of configurable properties accepted by applyJavaNature. */
  static class JavaNatureConfiguration {
    /** Controls whether the spotbugs plugin is enabled and configured. */
    boolean enableSpotbugs = true

    /** Regexes matching generated classes which should not receive extended type checking. */
    List<String> generatedClassPatterns = []

    /** Classes triggering Checker failures. A map from class name to the bug filed against checkerframework. */
    Map<String, String> classesTriggerCheckerBugs = [:]

    /** Controls whether the dependency analysis plugin is enabled. */
    boolean enableStrictDependencies = true

    /** Override the default "beam-" + `dash separated path` archivesBaseName. */
    String archivesBaseName = null

    /**
     * List of additional lint warnings to disable.
     * In addition, defaultLintSuppressions defined below
     * will be applied to all projects.
     */
    List<String> disableLintWarnings = []

    /** Controls whether tests are run with shadowJar. */
    boolean testShadowJar = false

    /**
     * Controls whether the shadow jar is validated to not contain any classes outside the org.apache.beam namespace.
     * This protects artifact jars from leaking dependencies classes causing conflicts for users.
     *
     * Note that this can be disabled for subprojects that produce application artifacts that are not intended to
     * be depended on by users.
     */
    boolean validateShadowJar = true

    /**
     * Controls whether 'jmh' specific configuration is enabled to build a JMH
     * focused module.
     *
     * Add additional dependencies to the implementation configuration.
     *
     * Note that the JMH annotation processor is enabled by default and that
     * a 'jmh' task is created which executes JMH.
     *
     * Publishing is not allowed for JMH enabled projects.
     */
    boolean enableJmh = false

    /**
     * The set of excludes that should be used during validation of the shadow jar. Projects should override
     * the default with the most specific set of excludes that is valid for the contents of its shaded jar.
     *
     * By default we exclude any class underneath the org.apache.beam namespace.
     */
    List<String> shadowJarValidationExcludes = ["org/apache/beam/**"]

    /**
     * If unset, no shading is performed. The jar and test jar archives are used during publishing.
     * Otherwise the shadowJar and shadowTestJar artifacts are used during publishing.
     *
     * The shadowJar / shadowTestJar tasks execute the specified closure to configure themselves.
     */
    Closure shadowClosure

    /** Controls whether this project is published to Maven. */
    boolean publish = true

    /** Controls whether javadoc is exported for this project. */
    boolean exportJavadoc = true

    /**
     * Automatic-Module-Name Header value to be set in MANFIEST.MF file.
     * This is a required parameter unless publishing to Maven is disabled for this project.
     *
     * @see: https://github.com/GoogleCloudPlatform/cloud-opensource-java/blob/master/library-best-practices/JLBP-20.md
     */
    String automaticModuleName = null

    /**
     * The set of additional maven repositories that should be added into published POM file.
     */
    List<Map> mavenRepositories = []

    /**
     * Set minimal Java version needed to compile the module.
     *
     * <p>Valid values are LTS versions greater than the lowest supported
     * Java version. Used when newer Java byte code version required than Beam's
     * byte code compatibility version.
     */
    JavaVersion requireJavaVersion = null
  }

  /** A class defining the set of configurable properties accepted by applyPortabilityNature. */
  static class PortabilityNatureConfiguration {
    /**
     * The set of excludes that should be used during validation of the shadow jar. Projects should override
     * the default with the most specific set of excludes that is valid for the contents of its shaded jar.
     *
     * By default we exclude any class underneath the org.apache.beam namespace.
     */
    List<String> shadowJarValidationExcludes = ["org/apache/beam/**"]

    /** Override the default "beam-" + `dash separated path` archivesBaseName. */
    String archivesBaseName = null

    /** Controls whether this project is published to Maven. */
    boolean publish = true

    /**
     * Regexes matching generated Java classes which should not receive extended type checking.
     *
     * By default, skips anything in the `org.apache.beam.model` namespace.
     */
    List<String> generatedClassPatterns = [
      "^org\\.apache\\.beam\\.model.*"
    ]

    /**
     * Automatic-Module-Name Header value to be set in MANFIEST.MF file.
     * This is a required parameter unless publishing to Maven is disabled for this project.
     *
     * @see: https://github.com/GoogleCloudPlatform/cloud-opensource-java/blob/master/library-best-practices/JLBP-20.md
     */
    String automaticModuleName
  }

  // A class defining the set of configurable properties for createJavaExamplesArchetypeValidationTask
  static class JavaExamplesArchetypeValidationConfiguration {
    // Type [Quickstart, MobileGaming] for the postrelease validation is required.
    // Used both for the test name run${type}Java${runner}
    // and also for the script name, ${type}-java-${runner}.toLowerCase().
    String type

    // runner [Direct, Dataflow, Spark, Flink, FlinkLocal]
    String runner

    // gcpProject sets the gcpProject argument when executing examples.
    String gcpProject

    // gcpRegion sets the region for executing Dataflow examples.
    String gcpRegion

    // gcsBucket sets the gcsProject argument when executing examples.
    String gcsBucket

    // bqDataset sets the BigQuery Dataset when executing mobile-gaming examples
    String bqDataset

    // pubsubTopic sets topics when executing streaming pipelines
    String pubsubTopic
  }

  // Reads and contains all necessary performance test parameters
  static class JavaPerformanceTestConfiguration {
    // Optional. Runner which will be used for running the tests. Possible values: dataflow/direct.
    // PerfKitBenchmarker will have trouble reading 'null' value. It expects empty string if no config file is expected.
    String runner = System.getProperty('integrationTestRunner', '')

    // Optional. Filesystem which will be used for running the tests. Possible values: hdfs.
    // if not specified runner's local filesystem will be used.
    String filesystem = System.getProperty('filesystem')

    // Required. Pipeline options to be used by the tested pipeline.
    String integrationTestPipelineOptions = System.getProperty('integrationTestPipelineOptions')
  }

  // Reads and contains all necessary performance test parameters
  static class PythonPerformanceTestConfiguration {
    // Fully qualified name of the test to run.
    String tests = System.getProperty('tests')

    // Attribute tag that can filter the test set.
    String attribute = System.getProperty('attr')

    // Extra test options pass to pytest.
    String[] extraTestOptions = ["--capture=no"]

    // Name of Cloud KMS encryption key to use in some tests.
    String kmsKeyName = System.getProperty('kmsKeyName')

    // Pipeline options to be used for pipeline invocation.
    String pipelineOptions = System.getProperty('pipelineOptions', '')
  }

  // A class defining the set of configurable properties accepted by containerImageName.
  static class ContainerImageNameConfiguration {
    String root = null // Sets the docker repository root (optional).
    String name = null // Sets the short container image name, such as "go" (required).
    String tag = null // Sets the image tag (optional).
  }

  // A class defining the configuration for PortableValidatesRunner.
  static class PortableValidatesRunnerConfiguration {
    // Task name for validate runner case.
    String name = 'validatesPortableRunner'
    // Fully qualified JobServerClass name to use.
    String jobServerDriver
    // A string representing the jobServer Configuration.
    String jobServerConfig
    // Number of parallel test runs.
    Integer numParallelTests = 1
    // Extra options to pass to TestPipeline
    String[] pipelineOpts = []
    // Spin up the Harness inside a DOCKER container
    Environment environment = Environment.DOCKER
    // Categories for tests to run.
    Closure testCategories = {
      includeCategories 'org.apache.beam.sdk.testing.ValidatesRunner'
      // Use the following to include / exclude categories:
      // includeCategories 'org.apache.beam.sdk.testing.ValidatesRunner'
      // excludeCategories 'org.apache.beam.sdk.testing.FlattenWithHeterogeneousCoders'
    }
    // Tests to include/exclude from running, by default all tests are included
    Closure testFilter = {
      // Use the following to include / exclude tests:
      // includeTestsMatching 'org.apache.beam.sdk.transforms.FlattenTest.testFlattenWithDifferentInputAndOutputCoders2'
      // excludeTestsMatching 'org.apache.beam.sdk.transforms.FlattenTest.testFlattenWithDifferentInputAndOutputCoders2'
    }
    // Configuration for the classpath when running the test.
    Configuration testClasspathConfiguration
    // Additional system properties.
    Properties systemProperties = []

    enum Environment {
      DOCKER,   // Docker-based Harness execution
      PROCESS,  // Process-based Harness execution
      EMBEDDED, // Execute directly inside the execution engine (testing only)
    }
  }

  // A class defining the common properties in a given suite of cross-language tests
  // Properties are shared across runners and are used when creating a CrossLanguageUsingJavaExpansionConfiguration object
  static class CrossLanguageTask {
    // Used as the task name for cross-language
    String name
    // List of project paths for required expansion services
    List<String> expansionProjectPaths
    // Collect Python pipeline tests with this marker
    String collectMarker
    // Additional environment variables to set before running tests
    Map<String,String> additionalEnvs
    // Additional Python dependencies to install before running tests
    List<String> additionalDeps
  }

  // A class defining the configuration for CrossLanguageUsingJavaExpansion.
  static class CrossLanguageUsingJavaExpansionConfiguration {
    // Task name for cross-language tests using Java expansion.
    String name = 'crossLanguageUsingJavaExpansion'
    // Python pipeline options to use.
    List<String> pythonPipelineOptions = [
      "--runner=PortableRunner",
      "--job_endpoint=localhost:8099",
      "--environment_cache_millis=10000",
      "--experiments=beam_fn_api",
    ]
    // Additional pytest options
    List<String> pytestOptions = []
    // Number of parallel test runs.
    Integer numParallelTests = 1
    // List of project paths for required expansion services
    List<String> expansionProjectPaths
    // Collect Python pipeline tests with this marker
    String collectMarker
    // any additional environment variables to be exported
    Map<String,String> additionalEnvs
    // Additional Python dependencies to install before running tests
    List<String> additionalDeps
  }

  // A class defining the configuration for CrossLanguageValidatesRunner.
  static class CrossLanguageValidatesRunnerConfiguration {
    // Task name for cross-language validate runner case.
    String name = 'validatesCrossLanguageRunner'
    // Java pipeline options to use.
    List<String> javaPipelineOptions = [
      "--runner=PortableRunner",
      "--jobEndpoint=localhost:8099",
      "--environmentCacheMillis=10000",
      "--experiments=beam_fn_api",
    ]
    // Python pipeline options to use.
    List<String> pythonPipelineOptions = [
      "--runner=PortableRunner",
      "--job_endpoint=localhost:8099",
      "--environment_cache_millis=10000",
      "--experiments=beam_fn_api",
    ]
    // Go script options to use.
    List<String> goScriptOptions = [
      "--runner portable",
      "--endpoint localhost:8099",
      "--tests \"./test/integration/xlang ./test/integration/io/xlang/...\""
    ]
    // Additional pytest options
    List<String> pytestOptions = []
    // Job server startup task.
    TaskProvider startJobServer
    // Job server cleanup task.
    TaskProvider cleanupJobServer
    // Number of parallel test runs.
    Integer numParallelTests = 1
    // Whether the pipeline needs --sdk_location option
    boolean needsSdkLocation = false
    // semi_persist_dir for SDK containers
    String semiPersistDir = "/tmp"
    // classpath for running tests.
    FileCollection classpath
  }

  // A class defining the configuration for createTransformServiceTask.
  static class TransformServiceConfiguration {
    // Task name TransformService case.
    String name = 'transformService'

    List<String> pythonPipelineOptions = []

    List<String> javaPipelineOptions = []

    // Additional pytest options
    List<String> pytestOptions = []
    // Job server startup task.
    TaskProvider startJobServer
    // Job server cleanup task.
    TaskProvider cleanupJobServer
    // Number of parallel test runs.
    Integer numParallelTests = 1
    // Whether the pipeline needs --sdk_location option
    boolean needsSdkLocation = false

    // Collect Python pipeline tests with this marker
    String collectMarker
  }

  def isRelease(Project project) {
    return parseBooleanProperty(project, 'isRelease');
  }
  /**
   * Parses -Pprop as true for use as a flag, and otherwise uses Groovy's toBoolean
   */
  def parseBooleanProperty(Project project, String property) {
    if (!project.hasProperty(property)) {
      return false;
    }

    if (project.getProperty(property) == "") {
      return true;
    }

    return project.getProperty(property).toBoolean();
  }

  def defaultArchivesBaseName(Project p) {
    return 'beam' + p.path.replace(':', '-')
  }

  /** Get version for Java SDK container */
  static def getSupportedJavaVersion(String assignedVersion = null) {
    JavaVersion ver = assignedVersion ? JavaVersion.toVersion(assignedVersion) : JavaVersion.current()
    if (ver <= JavaVersion.VERSION_11) {
      return 'java11'
    } else if (ver <= JavaVersion.VERSION_17) {
      return 'java17'
    } else {
      return 'java21'
    }
  }

  /*
   * Set compile args for compiling and running in different java version by modifying the compiler args in place.
   *
   * Replace `-source X` and `-target X` or `--release X` options if already existed in compilerArgs.
   */
  static def setCompileAndRuntimeJavaVersion(List<String> compilerArgs, String ver) {
    boolean foundS = false, foundT = false
    int foundR = -1
    logger.fine("set java ver ${ver} to compiler args")
    for (int i = 0; i < compilerArgs.size()-1; ++i) {
      if (compilerArgs.get(i) == '-source') {
        foundS = true
        compilerArgs.set(i+1, ver)
      } else if (compilerArgs.get(i) == '-target')  {
        foundT = true
        compilerArgs.set(i+1, ver)
      } else if (compilerArgs.get(i) == '--release') {
        foundR = i
      }
    }
    if (foundR != -1) {
      compilerArgs.removeAt(foundR + 1)
      compilerArgs.removeAt(foundR)
    }
    if (!foundS) {
      compilerArgs.addAll('-source', ver)
    }
    if (!foundT) {
      compilerArgs.addAll('-target', ver)
    }
  }

  void apply(Project project) {

    /** ***********************************************************************************************/
    // Apply common properties/repositories and tasks to all projects.

    project.ext.mavenGroupId = 'org.apache.beam'

    // Default to dash-separated directories for artifact base name,
    // which will also be the default artifactId for maven publications
    project.apply plugin: 'base'
    project.archivesBaseName = defaultArchivesBaseName(project)

    // Register all Beam repositories and configuration tweaks
    Repositories.register(project)

    // Apply a plugin which enables configuring projects imported into Intellij.
    project.apply plugin: "idea"

    // Provide code coverage
    // Enable when 'enableJacocoReport' project property is specified or when running ":javaPreCommit"
    project.apply plugin: "jacoco"
    project.gradle.taskGraph.whenReady { graph ->
      // Disable jacoco unless report requested such that task outputs can be properly cached.
      // https://discuss.gradle.org/t/do-not-cache-if-condition-matched-jacoco-agent-configured-with-append-true-satisfied/23504
      def enabled = project.hasProperty('enableJacocoReport') || graph.allTasks.any { it instanceof JacocoReport || it.name.contains('javaPreCommit') }
      project.tasks.withType(Test) { jacoco.enabled = enabled }
    }

    // Apply a plugin which provides tasks for dependency / property / task reports.
    // See https://docs.gradle.org/current/userguide/project_reports_plugin.html
    // for further details. This is typically very useful to look at the "htmlDependencyReport"
    // when attempting to resolve dependency issues.
    project.apply plugin: "project-report"

    // Apply a plugin which fails the build if there is a dependency on a transitive
    // non-declared dependency, since these can break users (as in BEAM-6558)
    //
    // Though this is Java-specific, it is required to be applied to the root
    // project due to implementation-details of the plugin. It can be enabled/disabled
    // via JavaNatureConfiguration per project. It is disabled by default until we can
    // make all of our deps good.
    project.apply plugin: "ca.cutterslade.analyze"

    // Adds a taskTree task that prints task dependency tree report to the console.
    // Useful for investigating build issues.
    // See: https://github.com/dorongold/gradle-task-tree
    project.apply plugin: "com.dorongold.task-tree"
    project.taskTree { noRepeat = true }

    project.ext.currentJavaVersion = getSupportedJavaVersion()

    project.ext.allFlinkVersions = project.flink_versions.split(',')
    project.ext.latestFlinkVersion = project.ext.allFlinkVersions.last()

    project.ext.nativeArchitecture = {
      // Best guess as to this system's normalized native architecture name.
      System.getProperty('os.arch') == 'aarch64' || System.getProperty('os.arch').contains('arm') ? "arm64" : "amd64"
    }

    project.ext.containerArchitectures = {
      if (isRelease(project)) {
        // Ensure we always publish the expected containers.
        return ["amd64", "arm64"]
      } else if (project.rootProject.findProperty("container-architecture-list") != null) {
        def containerArchitectures = project.rootProject.findProperty("container-architecture-list").split(',')
        if (containerArchitectures.size() > 1 && !project.rootProject.hasProperty("push-containers")) {
          throw new GradleException("A multi-arch image can't be saved in the local image store, please append the -Ppush-containers flag and specify a repository to push in the -Pdocker-repository-root flag.");
        }
        return containerArchitectures
      } else {
        return [project.nativeArchitecture()]
      }
    }

    project.ext.containerPlatforms = {
      return project.containerArchitectures().collect { arch -> "linux/" + arch }
    }

    project.ext.useBuildx = {
      return (project.containerArchitectures() != [project.nativeArchitecture()]) || project.rootProject.hasProperty("useDockerBuildx")
    }

    /** ***********************************************************************************************/
    // Define and export a map dependencies shared across multiple sub-projects.
    //
    // Example usage:
    // configuration {
    //   implementation library.java.avro
    //   testImplementation library.java.junit
    // }

    // These versions are defined here because they represent
    // a dependency version which should match across multiple
    // Maven artifacts.
    //
    // There are a few versions are determined by the BOMs by running scripts/tools/bomupgrader.py
    // marked as [bomupgrader]. See the documentation of that script for detail.
    def activemq_version = "5.14.5"
    def autovalue_version = "1.9"
    def autoservice_version = "1.0.1"
    def aws_java_sdk2_version = "2.20.162"
    def cassandra_driver_version = "3.10.2"
    def cdap_version = "6.5.1"
    def checkerframework_version = "3.42.0"
    def classgraph_version = "4.8.162"
    def dbcp2_version = "2.9.0"
    def errorprone_version = "2.10.0"
    // [bomupgrader] determined by: com.google.api:gax, consistent with: google_cloud_platform_libraries_bom
    def gax_version = "2.67.0"
    def google_ads_version = "33.0.0"
    def google_clients_version = "2.0.0"
    def google_cloud_bigdataoss_version = "2.2.26"
    // [bomupgrader] determined by: com.google.cloud:google-cloud-spanner, consistent with: google_cloud_platform_libraries_bom
    def google_cloud_spanner_version = "6.95.1"
    def google_code_gson_version = "2.10.1"
    def google_oauth_clients_version = "1.34.1"
    // [bomupgrader] determined by: io.grpc:grpc-netty, consistent with: google_cloud_platform_libraries_bom
    def grpc_version = "1.71.0"
    def guava_version = "33.1.0-jre"
    def hadoop_version = "3.4.1"
    def hamcrest_version = "2.1"
    def influxdb_version = "2.19"
    def httpclient_version = "4.5.13"
    def httpcore_version = "4.4.14"
    def iceberg_bqms_catalog_version = "1.6.1-1.0.1"
    def jackson_version = "2.15.4"
    def jaxb_api_version = "2.3.3"
    def jsr305_version = "3.0.2"
    def everit_json_version = "1.14.2"
    def kafka_version = "2.4.1"
    def log4j2_version = "2.20.0"
    def nemo_version = "0.1"
    // [bomupgrader] determined by: io.grpc:grpc-netty, consistent with: google_cloud_platform_libraries_bom
    def netty_version = "4.1.110.Final"
    def postgres_version = "42.2.16"
    // [bomupgrader] determined by: com.google.protobuf:protobuf-java, consistent with: google_cloud_platform_libraries_bom
    def protobuf_version = "4.29.4"
    def qpid_jms_client_version = "0.61.0"
    def quickcheck_version = "1.0"
    def sbe_tool_version = "1.25.1"
    def singlestore_jdbc_version = "1.1.4"
    def slf4j_version = "2.0.16"
    def snakeyaml_engine_version = "2.6"
    def snakeyaml_version = "2.2"
    def solace_version = "10.21.0"
    def spark2_version = "2.4.8"
    def spark3_version = "3.5.0"
    def spotbugs_version = "4.8.3"
    def testcontainers_version = "1.19.7"
    // [bomupgrader] determined by: org.apache.arrow:arrow-memory-core, consistent with: google_cloud_platform_libraries_bom
    def arrow_version = "15.0.2"
    def jmh_version = "1.34"
    def jupiter_version = "5.7.0"

    // Export Spark versions, so they are defined in a single place only
    project.ext.spark3_version = spark3_version
    // version for BigQueryMetastore catalog (used by sdks:java:io:iceberg:bqms)
    // TODO: remove this and download the jar normally when the catalog gets
    // open-sourced (https://github.com/apache/iceberg/pull/11039)
    project.ext.iceberg_bqms_catalog_version = iceberg_bqms_catalog_version

    // A map of maps containing common libraries used per language. To use:
    // dependencies {
    //   compile library.java.slf4j_api
    // }
    project.ext.library = [
      java : [
        activemq_amqp                               : "org.apache.activemq:activemq-amqp:$activemq_version",
        activemq_broker                             : "org.apache.activemq:activemq-broker:$activemq_version",
        activemq_client                             : "org.apache.activemq:activemq-client:$activemq_version",
        activemq_jaas                               : "org.apache.activemq:activemq-jaas:$activemq_version",
        activemq_junit                              : "org.apache.activemq.tooling:activemq-junit:$activemq_version",
        activemq_kahadb_store                       : "org.apache.activemq:activemq-kahadb-store:$activemq_version",
        activemq_mqtt                               : "org.apache.activemq:activemq-mqtt:$activemq_version",
        args4j                                      : "args4j:args4j:2.33",
        auto_value_annotations                      : "com.google.auto.value:auto-value-annotations:$autovalue_version",
        // TODO: https://github.com/apache/beam/issues/34993 after stopping supporting Java 8
        avro                                        : "org.apache.avro:avro:1.11.4",
        aws_java_sdk2_apache_client                 : "software.amazon.awssdk:apache-client:$aws_java_sdk2_version",
        aws_java_sdk2_netty_client                  : "software.amazon.awssdk:netty-nio-client:$aws_java_sdk2_version",
        aws_java_sdk2_auth                          : "software.amazon.awssdk:auth:$aws_java_sdk2_version",
        aws_java_sdk2_cloudwatch                    : "software.amazon.awssdk:cloudwatch:$aws_java_sdk2_version",
        aws_java_sdk2_dynamodb                      : "software.amazon.awssdk:dynamodb:$aws_java_sdk2_version",
        aws_java_sdk2_kinesis                       : "software.amazon.awssdk:kinesis:$aws_java_sdk2_version",
        aws_java_sdk2_sdk_core                      : "software.amazon.awssdk:sdk-core:$aws_java_sdk2_version",
        aws_java_sdk2_aws_core                      : "software.amazon.awssdk:aws-core:$aws_java_sdk2_version",
        aws_java_sdk2_sns                           : "software.amazon.awssdk:sns:$aws_java_sdk2_version",
        aws_java_sdk2_sqs                           : "software.amazon.awssdk:sqs:$aws_java_sdk2_version",
        aws_java_sdk2_sts                           : "software.amazon.awssdk:sts:$aws_java_sdk2_version",
        aws_java_sdk2_s3                            : "software.amazon.awssdk:s3:$aws_java_sdk2_version",
        aws_java_sdk2_http_client_spi               : "software.amazon.awssdk:http-client-spi:$aws_java_sdk2_version",
        aws_java_sdk2_regions                       : "software.amazon.awssdk:regions:$aws_java_sdk2_version",
        aws_java_sdk2_utils                         : "software.amazon.awssdk:utils:$aws_java_sdk2_version",
        aws_java_sdk2_profiles                      : "software.amazon.awssdk:profiles:$aws_java_sdk2_version",
        azure_sdk_bom                               : "com.azure:azure-sdk-bom:1.2.14",
        bigdataoss_gcsio                            : "com.google.cloud.bigdataoss:gcsio:$google_cloud_bigdataoss_version",
        bigdataoss_gcs_connector                    : "com.google.cloud.bigdataoss:gcs-connector:hadoop2-$google_cloud_bigdataoss_version",
        bigdataoss_util                             : "com.google.cloud.bigdataoss:util:$google_cloud_bigdataoss_version",
        bigdataoss_util_hadoop                      : "com.google.cloud.bigdataoss:util-hadoop:hadoop2-$google_cloud_bigdataoss_version",
        byte_buddy                                  : "net.bytebuddy:byte-buddy:1.14.12",
        cassandra_driver_core                       : "com.datastax.cassandra:cassandra-driver-core:$cassandra_driver_version",
        cassandra_driver_mapping                    : "com.datastax.cassandra:cassandra-driver-mapping:$cassandra_driver_version",
        cdap_api                                    : "io.cdap.cdap:cdap-api:$cdap_version",
        cdap_api_commons                            : "io.cdap.cdap:cdap-api-common:$cdap_version",
        cdap_common                                 : "io.cdap.cdap:cdap-common:$cdap_version",
        cdap_etl_api                                : "io.cdap.cdap:cdap-etl-api:$cdap_version",
        cdap_etl_api_spark                          : "io.cdap.cdap:cdap-etl-api-spark:$cdap_version",
        cdap_hydrator_common                        : "io.cdap.plugin:hydrator-common:2.4.0",
        cdap_plugin_hubspot                         : "io.cdap:hubspot-plugins:1.0.0",
        cdap_plugin_salesforce                      : "io.cdap.plugin:salesforce-plugins:1.4.0",
        cdap_plugin_service_now                     : "io.cdap.plugin:servicenow-plugins:1.1.0",
        cdap_plugin_zendesk                         : "io.cdap.plugin:zendesk-plugins:1.0.0",
        checker_qual                                : "org.checkerframework:checker-qual:$checkerframework_version",
        classgraph                                  : "io.github.classgraph:classgraph:$classgraph_version",
        commons_codec                               : "commons-codec:commons-codec:1.17.1",
        commons_collections                         : "commons-collections:commons-collections:3.2.2",
        commons_compress                            : "org.apache.commons:commons-compress:1.26.2",
        commons_csv                                 : "org.apache.commons:commons-csv:1.8",
        commons_io                                  : "commons-io:commons-io:2.16.1",
        commons_lang3                               : "org.apache.commons:commons-lang3:3.14.0",
        commons_logging                             : "commons-logging:commons-logging:1.2",
        commons_math3                               : "org.apache.commons:commons-math3:3.6.1",
        dbcp2                                       : "org.apache.commons:commons-dbcp2:$dbcp2_version",
        error_prone_annotations                     : "com.google.errorprone:error_prone_annotations:$errorprone_version",
        failsafe                                    : "dev.failsafe:failsafe:3.3.0",
        flogger_system_backend                      : "com.google.flogger:flogger-system-backend:0.7.4",
        gax                                         : "com.google.api:gax", // google_cloud_platform_libraries_bom sets version
        gax_grpc                                    : "com.google.api:gax-grpc", // google_cloud_platform_libraries_bom sets version
        gax_grpc_test                               : "com.google.api:gax-grpc:$gax_version:testlib", // google_cloud_platform_libraries_bom sets version
        gax_httpjson                                : "com.google.api:gax-httpjson", // google_cloud_platform_libraries_bom sets version
        google_api_client                           : "com.google.api-client:google-api-client:$google_clients_version", // for the libraries using $google_clients_version below.
        google_api_client_gson                      : "com.google.api-client:google-api-client-gson:$google_clients_version",
        google_api_client_java6                     : "com.google.api-client:google-api-client-java6:$google_clients_version",
        google_api_common                           : "com.google.api:api-common", // google_cloud_platform_libraries_bom sets version
        google_api_services_bigquery                : "com.google.apis:google-api-services-bigquery:v2-rev20250511-2.0.0",  // [bomupgrader] sets version
        google_api_services_cloudresourcemanager    : "com.google.apis:google-api-services-cloudresourcemanager:v1-rev20240310-2.0.0",  // [bomupgrader] sets version
        google_api_services_dataflow                : "com.google.apis:google-api-services-dataflow:v1b3-rev20250519-$google_clients_version",
        google_api_services_healthcare              : "com.google.apis:google-api-services-healthcare:v1-rev20240130-$google_clients_version",
        google_api_services_pubsub                  : "com.google.apis:google-api-services-pubsub:v1-rev20220904-$google_clients_version",
        google_api_services_storage                 : "com.google.apis:google-api-services-storage:v1-rev20250524-2.0.0",  // [bomupgrader] sets version
        google_auth_library_credentials             : "com.google.auth:google-auth-library-credentials", // google_cloud_platform_libraries_bom sets version
        google_auth_library_oauth2_http             : "com.google.auth:google-auth-library-oauth2-http", // google_cloud_platform_libraries_bom sets version
        google_cloud_bigquery                       : "com.google.cloud:google-cloud-bigquery", // google_cloud_platform_libraries_bom sets version
        google_cloud_bigquery_storage               : "com.google.cloud:google-cloud-bigquerystorage", // google_cloud_platform_libraries_bom sets version
        google_cloud_bigtable                       : "com.google.cloud:google-cloud-bigtable", // google_cloud_platform_libraries_bom sets version
        google_cloud_bigtable_client_core_config    : "com.google.cloud.bigtable:bigtable-client-core-config:1.28.0",
        google_cloud_bigtable_emulator              : "com.google.cloud:google-cloud-bigtable-emulator", // google_cloud_platform_libraries_bom sets version
        google_cloud_core                           : "com.google.cloud:google-cloud-core", // google_cloud_platform_libraries_bom sets version
        google_cloud_core_grpc                      : "com.google.cloud:google-cloud-core-grpc", // google_cloud_platform_libraries_bom sets version
        google_cloud_datacatalog_v1beta1            : "com.google.cloud:google-cloud-datacatalog", // google_cloud_platform_libraries_bom sets version
        google_cloud_dataflow_java_proto_library_all: "com.google.cloud.dataflow:google-cloud-dataflow-java-proto-library-all:0.5.160304",
        google_cloud_datastore_v1_proto_client      : "com.google.cloud.datastore:datastore-v1-proto-client:2.29.1",   // [bomupgrader] sets version
        google_cloud_firestore                      : "com.google.cloud:google-cloud-firestore", // google_cloud_platform_libraries_bom sets version
        google_cloud_pubsub                         : "com.google.cloud:google-cloud-pubsub", // google_cloud_platform_libraries_bom sets version
        google_cloud_pubsublite                     : "com.google.cloud:google-cloud-pubsublite",  // google_cloud_platform_libraries_bom sets version
        // [bomupgrader] the BOM version is set by scripts/tools/bomupgrader.py. If update manually, also update
        // libraries-bom version on sdks/java/container/license_scripts/dep_urls_java.yaml
        google_cloud_platform_libraries_bom         : "com.google.cloud:libraries-bom:26.62.0",
        google_cloud_secret_manager                 : "com.google.cloud:google-cloud-secretmanager", // google_cloud_platform_libraries_bom sets version
        google_cloud_spanner                        : "com.google.cloud:google-cloud-spanner", // google_cloud_platform_libraries_bom sets version
        google_cloud_spanner_test                   : "com.google.cloud:google-cloud-spanner:$google_cloud_spanner_version:tests",
        google_cloud_vertexai                       : "com.google.cloud:google-cloud-vertexai", // google_cloud_platform_libraries_bom sets version
        google_code_gson                            : "com.google.code.gson:gson:$google_code_gson_version",
        // google-http-client's version is explicitly declared for sdks/java/maven-archetypes/examples
        google_http_client                          : "com.google.http-client:google-http-client", // google_cloud_platform_libraries_bom sets version
        google_http_client_apache_v2                : "com.google.http-client:google-http-client-apache-v2", // google_cloud_platform_libraries_bom sets version
        google_http_client_gson                     : "com.google.http-client:google-http-client-gson", // google_cloud_platform_libraries_bom sets version
        google_http_client_jackson                  : "com.google.http-client:google-http-client-jackson:1.29.2",
        google_http_client_gson                     : "com.google.http-client:google-http-client-gson", // google_cloud_platform_libraries_bom sets version
        google_http_client_protobuf                 : "com.google.http-client:google-http-client-protobuf", // google_cloud_platform_libraries_bom sets version
        google_oauth_client                         : "com.google.oauth-client:google-oauth-client:$google_oauth_clients_version",
        google_oauth_client_java6                   : "com.google.oauth-client:google-oauth-client-java6:$google_oauth_clients_version",
        // Don't use grpc_all, it can cause issues in Bazel builds. Reference the gRPC libraries you need individually instead.
        grpc_alts                                   : "io.grpc:grpc-alts", // google_cloud_platform_libraries_bom sets version
        grpc_api                                    : "io.grpc:grpc-api", // google_cloud_platform_libraries_bom sets version
        grpc_auth                                   : "io.grpc:grpc-auth", // google_cloud_platform_libraries_bom sets version
        grpc_census                                 : "io.grpc:grpc-census", // google_cloud_platform_libraries_bom sets version
        grpc_context                                : "io.grpc:grpc-context", // google_cloud_platform_libraries_bom sets version
        grpc_core                                   : "io.grpc:grpc-core", // google_cloud_platform_libraries_bom sets version
        grpc_google_cloud_firestore_v1              : "com.google.api.grpc:grpc-google-cloud-firestore-v1", // google_cloud_platform_libraries_bom sets version
        grpc_google_cloud_pubsub_v1                 : "com.google.api.grpc:grpc-google-cloud-pubsub-v1", // google_cloud_platform_libraries_bom sets version
        grpc_google_cloud_pubsublite_v1             : "com.google.api.grpc:grpc-google-cloud-pubsublite-v1",  // google_cloud_platform_libraries_bom sets version
        grpc_google_common_protos                   : "com.google.api.grpc:grpc-google-common-protos", // google_cloud_platform_libraries_bom sets version
        grpc_grpclb                                 : "io.grpc:grpc-grpclb", // google_cloud_platform_libraries_bom sets version
        grpc_protobuf                               : "io.grpc:grpc-protobuf", // google_cloud_platform_libraries_bom sets version
        grpc_protobuf_lite                          : "io.grpc:grpc-protobuf-lite", // google_cloud_platform_libraries_bom sets version
        grpc_netty                                  : "io.grpc:grpc-netty", // google_cloud_platform_libraries_bom sets version
        grpc_netty_shaded                           : "io.grpc:grpc-netty-shaded", // google_cloud_platform_libraries_bom sets version
        grpc_stub                                   : "io.grpc:grpc-stub", // google_cloud_platform_libraries_bom sets version
        grpc_xds                                    : "io.grpc:grpc-xds", // google_cloud_platform_libraries_bom sets version
        guava                                       : "com.google.guava:guava:$guava_version",
        guava_testlib                               : "com.google.guava:guava-testlib:$guava_version",
        hadoop_auth                                 : "org.apache.hadoop:hadoop-auth:$hadoop_version",
        hadoop_client                               : "org.apache.hadoop:hadoop-client:$hadoop_version",
        hadoop_common                               : "org.apache.hadoop:hadoop-common:$hadoop_version",
        hadoop_mapreduce_client_core                : "org.apache.hadoop:hadoop-mapreduce-client-core:$hadoop_version",
        hadoop_minicluster                          : "org.apache.hadoop:hadoop-minicluster:$hadoop_version",
        hadoop_hdfs                                 : "org.apache.hadoop:hadoop-hdfs:$hadoop_version",
        hadoop_hdfs_client                          : "org.apache.hadoop:hadoop-hdfs-client:$hadoop_version",
        hadoop_hdfs_tests                           : "org.apache.hadoop:hadoop-hdfs:$hadoop_version:tests",
        hamcrest                                    : "org.hamcrest:hamcrest:$hamcrest_version",
        http_client                                 : "org.apache.httpcomponents:httpclient:$httpclient_version",
        http_core                                   : "org.apache.httpcomponents:httpcore:$httpcore_version",
        influxdb_library                            : "org.influxdb:influxdb-java:$influxdb_version",
        jackson_annotations                         : "com.fasterxml.jackson.core:jackson-annotations:$jackson_version",
        jackson_jaxb_annotations                    : "com.fasterxml.jackson.module:jackson-module-jaxb-annotations:$jackson_version",
        jackson_core                                : "com.fasterxml.jackson.core:jackson-core:$jackson_version",
        jackson_databind                            : "com.fasterxml.jackson.core:jackson-databind:$jackson_version",
        jackson_dataformat_cbor                     : "com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:$jackson_version",
        jackson_dataformat_csv                      : "com.fasterxml.jackson.dataformat:jackson-dataformat-csv:$jackson_version",
        jackson_dataformat_xml                      : "com.fasterxml.jackson.dataformat:jackson-dataformat-xml:$jackson_version",
        jackson_dataformat_yaml                     : "com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jackson_version",
        jackson_datatype_joda                       : "com.fasterxml.jackson.datatype:jackson-datatype-joda:$jackson_version",
        jackson_datatype_jsr310                     : "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jackson_version",
        jackson_module_scala_2_11                   : "com.fasterxml.jackson.module:jackson-module-scala_2.11:$jackson_version",
        jackson_module_scala_2_12                   : "com.fasterxml.jackson.module:jackson-module-scala_2.12:$jackson_version",
        jamm                                        : 'com.github.jbellis:jamm:0.4.0',
        jaxb_api                                    : "jakarta.xml.bind:jakarta.xml.bind-api:$jaxb_api_version",
        jaxb_impl                                   : "com.sun.xml.bind:jaxb-impl:$jaxb_api_version",
        jcl_over_slf4j                              : "org.slf4j:jcl-over-slf4j:$slf4j_version",
        jmh_core                                    : "org.openjdk.jmh:jmh-core:$jmh_version",
        joda_time                                   : "joda-time:joda-time:2.10.14",
        jsonassert                                  : "org.skyscreamer:jsonassert:1.5.0",
        jsr305                                      : "com.google.code.findbugs:jsr305:$jsr305_version",
        json_org                                    : "org.json:json:20231013", // Keep in sync with everit-json-schema / google_cloud_platform_libraries_bom transitive deps.
        everit_json_schema                          : "com.github.erosb:everit-json-schema:$everit_json_version",
        junit                                       : "junit:junit:4.13.1",
        jupiter_api                                 : "org.junit.jupiter:junit-jupiter-api:$jupiter_version",
        jupiter_engine                              : "org.junit.jupiter:junit-jupiter-engine:$jupiter_version",
        jupiter_params                              : "org.junit.jupiter:junit-jupiter-params:$jupiter_version",
        kafka                                       : "org.apache.kafka:kafka_2.11:$kafka_version",
        kafka_clients                               : "org.apache.kafka:kafka-clients:$kafka_version",
        log4j                                       : "log4j:log4j:1.2.17",
        log4j_over_slf4j                            : "org.slf4j:log4j-over-slf4j:$slf4j_version",
        log4j2_api                                  : "org.apache.logging.log4j:log4j-api:$log4j2_version",
        log4j2_core                                 : "org.apache.logging.log4j:log4j-core:$log4j2_version",
        log4j2_to_slf4j                             : "org.apache.logging.log4j:log4j-to-slf4j:$log4j2_version",
        log4j2_slf4j_impl                           : "org.apache.logging.log4j:log4j-slf4j-impl:$log4j2_version",
        log4j2_slf4j2_impl                          : "org.apache.logging.log4j:log4j-slf4j2-impl:$log4j2_version",
        log4j2_log4j12_api                          : "org.apache.logging.log4j:log4j-1.2-api:$log4j2_version",
        mockito_core                                : "org.mockito:mockito-core:4.11.0",
        mockito_inline                              : "org.mockito:mockito-inline:4.11.0",
        mongo_java_driver                           : "org.mongodb:mongo-java-driver:3.12.11",
        nemo_compiler_frontend_beam                 : "org.apache.nemo:nemo-compiler-frontend-beam:$nemo_version",
        netty_all                                   : "io.netty:netty-all:$netty_version",
        netty_handler                               : "io.netty:netty-handler:$netty_version",
        netty_tcnative_boringssl_static             : "io.netty:netty-tcnative-boringssl-static:2.0.52.Final",
        netty_transport                             : "io.netty:netty-transport:$netty_version",
        netty_transport_native_epoll                : "io.netty:netty-transport-native-epoll:$netty_version",
        postgres                                    : "org.postgresql:postgresql:$postgres_version",
        protobuf_java                               : "com.google.protobuf:protobuf-java:$protobuf_version",
        protobuf_java_util                          : "com.google.protobuf:protobuf-java-util:$protobuf_version",
        proto_google_cloud_bigquery_storage_v1      : "com.google.api.grpc:proto-google-cloud-bigquerystorage-v1", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_bigtable_admin_v2        : "com.google.api.grpc:proto-google-cloud-bigtable-admin-v2", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_bigtable_v2              : "com.google.api.grpc:proto-google-cloud-bigtable-v2", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_datacatalog_v1beta1      : "com.google.api.grpc:proto-google-cloud-datacatalog-v1beta1", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_datastore_v1             : "com.google.api.grpc:proto-google-cloud-datastore-v1", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_firestore_v1             : "com.google.api.grpc:proto-google-cloud-firestore-v1", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_pubsub_v1                : "com.google.api.grpc:proto-google-cloud-pubsub-v1", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_pubsublite_v1            : "com.google.api.grpc:proto-google-cloud-pubsublite-v1", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_secret_manager_v1        : "com.google.api.grpc:proto-google-cloud-secretmanager-v1", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_spanner_v1               : "com.google.api.grpc:proto-google-cloud-spanner-v1", // google_cloud_platform_libraries_bom sets version
        proto_google_cloud_spanner_admin_database_v1: "com.google.api.grpc:proto-google-cloud-spanner-admin-database-v1", // google_cloud_platform_libraries_bom sets version
        proto_google_common_protos                  : "com.google.api.grpc:proto-google-common-protos", // google_cloud_platform_libraries_bom sets version
        qpid_jms_client                             : "org.apache.qpid:qpid-jms-client:$qpid_jms_client_version",
        sbe_tool                                    : "uk.co.real-logic:sbe-tool:$sbe_tool_version",
        singlestore_jdbc                            : "com.singlestore:singlestore-jdbc-client:$singlestore_jdbc_version",
        slf4j_api                                   : "org.slf4j:slf4j-api:$slf4j_version",
        snake_yaml                                  : "org.yaml:snakeyaml:$snakeyaml_version",
        snakeyaml_engine                            : "org.snakeyaml:snakeyaml-engine:$snakeyaml_engine_version",
        slf4j_android                               : "org.slf4j:slf4j-android:$slf4j_version",
        slf4j_ext                                   : "org.slf4j:slf4j-ext:$slf4j_version",
        slf4j_jdk14                                 : "org.slf4j:slf4j-jdk14:$slf4j_version",
        slf4j_nop                                   : "org.slf4j:slf4j-nop:$slf4j_version",
        slf4j_simple                                : "org.slf4j:slf4j-simple:$slf4j_version",
        slf4j_jul_to_slf4j                          : "org.slf4j:jul-to-slf4j:$slf4j_version",
        slf4j_log4j12                               : "org.slf4j:slf4j-log4j12:$slf4j_version",
        slf4j_jcl                                   : "org.slf4j:slf4j-jcl:$slf4j_version",
        snappy_java                                 : "org.xerial.snappy:snappy-java:1.1.10.4",
        solace                                      : "com.solacesystems:sol-jcsmp:$solace_version",
        spark_core                                  : "org.apache.spark:spark-core_2.11:$spark2_version",
        spark_streaming                             : "org.apache.spark:spark-streaming_2.11:$spark2_version",
        spark3_core                                 : "org.apache.spark:spark-core_2.12:$spark3_version",
        spark3_network_common                       : "org.apache.spark:spark-network-common_2.12:$spark3_version",
        spark3_sql                                  : "org.apache.spark:spark-sql_2.12:$spark3_version",
        spark3_streaming                            : "org.apache.spark:spark-streaming_2.12:$spark3_version",
        stax2_api                                   : "org.codehaus.woodstox:stax2-api:4.2.1",
        tephra                                      : "org.apache.tephra:tephra-api:0.15.0-incubating",
        testcontainers_azure                        : "org.testcontainers:azure:$testcontainers_version",
        testcontainers_base                         : "org.testcontainers:testcontainers:$testcontainers_version",
        testcontainers_cassandra                    : "org.testcontainers:cassandra:$testcontainers_version",
        testcontainers_clickhouse                   : "org.testcontainers:clickhouse:$testcontainers_version",
        testcontainers_elasticsearch                : "org.testcontainers:elasticsearch:$testcontainers_version",
        testcontainers_gcloud                       : "org.testcontainers:gcloud:$testcontainers_version",
        testcontainers_jdbc                         : "org.testcontainers:jdbc:$testcontainers_version",
        testcontainers_kafka                        : "org.testcontainers:kafka:$testcontainers_version",
        testcontainers_localstack                   : "org.testcontainers:localstack:$testcontainers_version",
        testcontainers_mongodb                      : "org.testcontainers:mongodb:$testcontainers_version",
        testcontainers_mssqlserver                  : "org.testcontainers:mssqlserver:$testcontainers_version",
        testcontainers_mysql                        : "org.testcontainers:mysql:$testcontainers_version",
        testcontainers_neo4j                        : "org.testcontainers:neo4j:$testcontainers_version",
        testcontainers_oracle                       : "org.testcontainers:oracle-xe:$testcontainers_version",
        testcontainers_postgresql                   : "org.testcontainers:postgresql:$testcontainers_version",
        testcontainers_rabbitmq                     : "org.testcontainers:rabbitmq:$testcontainers_version",
        testcontainers_solace                       : "org.testcontainers:solace:$testcontainers_version",
        truth                                       : "com.google.truth:truth:1.1.5",
        threetenbp                                  : "org.threeten:threetenbp:1.6.8",
        vendored_grpc_1_69_0                        : "org.apache.beam:beam-vendor-grpc-1_69_0:0.1",
        vendored_guava_32_1_2_jre                   : "org.apache.beam:beam-vendor-guava-32_1_2-jre:0.1",
        vendored_calcite_1_40_0                     : "org.apache.beam:beam-vendor-calcite-1_40_0:0.1",
        woodstox_core_asl                           : "org.codehaus.woodstox:woodstox-core-asl:4.4.1",
        zstd_jni                                    : "com.github.luben:zstd-jni:1.5.6-3",
        quickcheck_core                             : "com.pholser:junit-quickcheck-core:$quickcheck_version",
        quickcheck_generators                       : "com.pholser:junit-quickcheck-generators:$quickcheck_version",
        arrow_vector                                : "org.apache.arrow:arrow-vector:$arrow_version",
        arrow_memory_core                           : "org.apache.arrow:arrow-memory-core:$arrow_version",
        arrow_memory_netty                          : "org.apache.arrow:arrow-memory-netty:$arrow_version",
      ],
      groovy: [
        groovy_all: "org.codehaus.groovy:groovy-all:2.4.13",
      ],
      // For generating pom.xml from archetypes.
      maven: [
        maven_compiler_plugin: "maven-plugins:maven-compiler-plugin:3.7.0",
        maven_exec_plugin    : "maven-plugins:maven-exec-plugin:1.6.0",
        maven_jar_plugin     : "maven-plugins:maven-jar-plugin:3.0.2",
        maven_shade_plugin   : "maven-plugins:maven-shade-plugin:3.1.0",
        maven_surefire_plugin: "maven-plugins:maven-surefire-plugin:3.0.0-M5",
      ],
    ]

    /** ***********************************************************************************************/

    // Returns a string representing the relocated path to be used with the shadow plugin when
    // given a suffix such as "com.google.common".
    project.ext.getJavaRelocatedPath = { String suffix ->
      return ("org.apache.beam.repackaged."
          + project.name.replace("-", "_")
          + "."
          + suffix)
    }

    def errorProneAddModuleOpts = [
      "--add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED",
      "--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
      "--add-exports=jdk.compiler/com.sun.tools.javac.main=ALL-UNNAMED",
      "--add-exports=jdk.compiler/com.sun.tools.javac.model=ALL-UNNAMED",
      "--add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED",
      "--add-exports=jdk.compiler/com.sun.tools.javac.processing=ALL-UNNAMED",
      "--add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED",
      "--add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED",
      "--add-opens=jdk.compiler/com.sun.tools.javac.code=ALL-UNNAMED",
      "--add-opens=jdk.compiler/com.sun.tools.javac.comp=ALL-UNNAMED"
    ]

    // set compiler options for java version overrides to compile with a different java version
    project.ext.setJavaVerOptions = { CompileOptions options, String ver ->
      if (ver == '8') {
        def java8Home = project.findProperty("java8Home")
        options.fork = true
        options.forkOptions.javaHome = java8Home as File
        options.compilerArgs += ['-Xlint:-path']
      } else if (ver == '11') {
        def java11Home = project.findProperty("java11Home")
        options.fork = true
        options.forkOptions.javaHome = java11Home as File
        options.compilerArgs += ['-Xlint:-path']
      } else if (ver == '17') {
        def java17Home = project.findProperty("java17Home")
        options.fork = true
        options.forkOptions.javaHome = java17Home as File
        options.compilerArgs += ['-Xlint:-path']
        // Error prone requires some packages to be exported/opened for Java 17
        // Disabling checks since this property is only used for tests
        // https://github.com/tbroyer/gradle-errorprone-plugin#jdk-16-support
        options.errorprone.errorproneArgs.add("-XepDisableAllChecks")
        // The -J prefix is needed to workaround https://github.com/gradle/gradle/issues/22747
        options.forkOptions.jvmArgs += errorProneAddModuleOpts.collect { '-J' + it }
      } else if (ver == '21') {
        def java21Home = project.findProperty("java21Home")
        options.fork = true
        options.forkOptions.javaHome = java21Home as File
        options.compilerArgs += [
          '-Xlint:-path',
          '-Xlint:-this-escape'
        ]
        // Error prone requires some packages to be exported/opened for Java 17+
        // Disabling checks since this property is only used for tests
        options.errorprone.errorproneArgs.add("-XepDisableAllChecks")
        options.forkOptions.jvmArgs += errorProneAddModuleOpts.collect { '-J' + it }
        // TODO(https://github.com/apache/beam/issues/28963)
        // upgrade checkerFramework to enable it in Java 21
        project.checkerFramework {
          skipCheckerFramework = true
        }
      } else {
        throw new GradleException("Unknown Java Version ${ver} for setting additional java options")
      }
    }

    project.ext.repositories = {
      maven {
        name "testPublicationLocal"
        url "file://${project.rootProject.projectDir}/testPublication/"
      }
      maven {
        url(project.properties['distMgmtSnapshotsUrl'] ?: isRelease(project)
            ? 'https://repository.apache.org/service/local/staging/deploy/maven2'
            : 'https://repository.apache.org/content/repositories/snapshots')
        name(project.properties['distMgmtServerId'] ?: isRelease(project)
            ? 'apache.releases.https' : 'apache.snapshots.https')
        // The maven settings plugin will load credentials from ~/.m2/settings.xml file that a user
        // has configured with the Apache release and snapshot staging credentials.
        // <settings>
        //   <servers>
        //     <server>
        //       <id>apache.releases.https</id>
        //       <username>USER_TOKEN</username>
        //       <password>PASS_TOKEN</password>
        //     </server>
        //     <server>
        //       <id>apache.snapshots.https</id>
        //       <username>USER_TOKEN</username>
        //       <password>PASS_TOKEN</password>
        //     </server>
        //   </servers>
        // </settings>
      }
    }

    // Configures a project with a default set of plugins that should apply to all Java projects.
    //
    // Users should invoke this method using Groovy map syntax. For example:
    // applyJavaNature(enableSpotbugs: true)
    //
    // See JavaNatureConfiguration for the set of accepted properties.
    //
    // The following plugins are enabled:
    //  * java
    //  * maven
    //  * net.ltgt.apt (plugin to configure annotation processing tool)
    //  * checkstyle
    //  * spotbugs
    //  * shadow (conditional on shadowClosure being specified)
    //  * com.diffplug.spotless (code style plugin)
    //
    // Dependency Management for Java Projects
    // ---------------------------------------
    //
    // By default, the shadow plugin is not enabled. It is only enabled by specifying a shadowClosure
    // as an argument. If no shadowClosure has been specified, dependencies should fall into the
    // configurations as described within the Gradle documentation (https://docs.gradle.org/current/userguide/java_plugin.html#sec:java_plugin_and_dependency_management)
    //
    // When the shadowClosure argument is specified, the shadow plugin is enabled to perform shading
    // of commonly found dependencies. Because of this it is important that dependencies are added
    // to the correct configuration. Dependencies should fall into one of these four configurations:
    //  * implementation     - Required during compilation or runtime of the main source set.
    //                         This configuration represents all dependencies that must also be shaded away
    //                         otherwise the generated Maven pom will be missing this dependency.
    //  * shadow             - Required during compilation or runtime of the main source set.
    //                         Will become a runtime dependency of the generated Maven pom.
    //  * testImplementation - Required during compilation or runtime of the test source set.
    //                         This must be shaded away in the shaded test jar.
    //  * shadowTest  -        Required during compilation or runtime of the test source set.
    //                         TODO: Figure out whether this should be a test scope dependency
    //                         of the generated Maven pom.
    //
    // When creating a cross-project dependency between two Java projects, one should only rely on
    // the shaded configurations if the project has a shadowClosure being specified. This allows
    // for compilation/test execution to occur against the final artifact that will be provided to
    // users. This is by done by referencing the "shadow" or "shadowTest" configuration as so:
    //   dependencies {
    //     shadow project(path: "other:java:project1", configuration: "shadow")
    //     shadowTest project(path: "other:java:project2", configuration: "shadowTest")
    //   }
    // This will ensure the correct set of transitive dependencies from those projects are correctly
    // added to the main and test source set runtimes.

    project.ext.applyJavaNature = {
      // Use the implicit it parameter of the closure to handle zero argument or one argument map calls.
      JavaNatureConfiguration configuration = it ? it as JavaNatureConfiguration : new JavaNatureConfiguration()

      // Validate configuration
      if (configuration.enableJmh && configuration.publish) {
        throw new GradleException("Publishing of a benchmark project is not allowed. Benchmark projects are not meant to be consumed as artifacts for end users.");
      }

      if (configuration.archivesBaseName) {
        project.archivesBaseName = configuration.archivesBaseName
      }

      project.apply plugin: "java"

      // We create a testRuntimeMigration configuration here to extend
      // testImplementation, testRuntimeOnly, and default (similar to what
      // testRuntime did).
      project.configurations {
        testRuntimeMigration.extendsFrom(project.configurations.default)
        testRuntimeMigration.extendsFrom(testImplementation)
        testRuntimeMigration.extendsFrom(testRuntimeOnly)
      }

      // Provided configuration to match Maven provided scope
      project.configurations {
        provided
        compileOnly.extendsFrom(provided)
        runtimeOnly.extendsFrom(provided)
      }

      // Configure the Java compiler source language and target compatibility levels. Also ensure that
      def requireJavaVersion = JavaVersion.toVersion(project.javaVersion)
      if (configuration.requireJavaVersion != null) {
        // Overwrite project.javaVersion if requested.
        if (JavaVersion.VERSION_11.equals(configuration.requireJavaVersion)) {
          project.javaVersion = '11'
        } else if (JavaVersion.VERSION_17.equals(configuration.requireJavaVersion)) {
          project.javaVersion = '17'
        } else if (JavaVersion.VERSION_21.equals(configuration.requireJavaVersion)) {
          project.javaVersion = '21'
        } else {
          throw new GradleException(
          "requireJavaVersion has to be supported LTS version greater than the default Java version. Actual: " +
          configuration.requireJavaVersion
          )
        }
        requireJavaVersion = configuration.requireJavaVersion
      }

      String forkJavaVersion = null
      if (requireJavaVersion.compareTo(JavaVersion.current()) > 0) {
        // If compiled on older SDK, compile with JDK configured with compatible javaXXHome
        // The order is intended here
        if (requireJavaVersion.compareTo(JavaVersion.VERSION_11) <= 0 &&
        project.hasProperty('java11Home')) {
          forkJavaVersion = '11'
        } else if (requireJavaVersion.compareTo(JavaVersion.VERSION_17) <= 0 &&
        project.hasProperty('java17Home')) {
          forkJavaVersion = '17'
        } else if (requireJavaVersion.compareTo(JavaVersion.VERSION_21) <= 0 &&
        project.hasProperty('java21Home')) {
          forkJavaVersion = '21'
        } else {
          logger.config("Module ${project.name} disabled. To enable, either " +
              "compile on newer Java version or pass java${project.javaVersion}Home project property")
          forkJavaVersion = ''
        }
      }

      project.sourceCompatibility = project.javaVersion
      project.targetCompatibility = project.javaVersion

      def defaultLintSuppressions = [
        'options',
        'cast',
        // https://bugs.openjdk.java.net/browse/JDK-8190452
        'classfile',
        'deprecation',
        'fallthrough',
        'processing',
        'serial',
        'try',
        'unchecked',
        'varargs',
      ]
      // Java21 introduced new lint "this-escape", violated by generated srcs
      // TODO(yathu) remove this once generated code (antlr) no longer trigger this warning
      if (JavaVersion.current().compareTo(JavaVersion.VERSION_21) >= 0) {
        defaultLintSuppressions += ['this-escape']
      }

      // Configure the default test tasks set of tests executed
      // to match the equivalent set that is executed by the maven-surefire-plugin.
      // See http://maven.apache.org/components/surefire/maven-surefire-plugin/test-mojo.html
      project.test {
        include "**/Test*.class"
        include "**/*Test.class"
        include "**/*Tests.class"
        include "**/*TestCase.class"
        // fixes issues with test filtering on multi-module project
        // see https://discuss.gradle.org/t/multi-module-build-fails-with-tests-filter/25835
        filter { setFailOnNoMatchingTests(false) }
      }

      project.tasks.withType(Test).configureEach {
        // Configure all test tasks to use JUnit
        useJUnit {}
        // default maxHeapSize on gradle 5 is 512m, lets increase to handle more demanding tests
        maxHeapSize = '2g'
      }

      List<String> skipDefRegexes = []
      skipDefRegexes << "AutoValue_.*"
      skipDefRegexes << "AutoOneOf_.*"
      skipDefRegexes << ".*\\.jmh_generated\\..*"
      skipDefRegexes += configuration.generatedClassPatterns
      skipDefRegexes += configuration.classesTriggerCheckerBugs.keySet()
      String skipDefCombinedRegex = skipDefRegexes.collect({ regex -> "(${regex})"}).join("|")

      List<String> skipUsesRegexes = []
      // zstd-jni is not annotated, handles Zstd(De)CompressCtx.loadDict(null) just fine
      skipUsesRegexes << "^com\\.github\\.luben\\.zstd\\..*"
      // SLF4J logger handles null log message parameters
      skipUsesRegexes << "^org\\.slf4j\\.Logger.*"
      String skipUsesCombinedRegex = skipUsesRegexes.collect({ regex -> "(${regex})"}).join("|")

      project.apply plugin: 'org.checkerframework'
      project.checkerFramework {
        checkers = [
          'org.checkerframework.checker.nullness.NullnessChecker'
        ]

        // Only skip checkerframework if explicitly requested
        skipCheckerFramework = project.hasProperty('enableCheckerFramework') &&
            !parseBooleanProperty(project, 'enableCheckerFramework')

        // Always exclude checkerframework on tests. It's slow, and it often
        // raises erroneous error because we don't have checker annotations for
        // test libraries like junit and hamcrest. See BEAM-11436.
        // Consider re-enabling if we can get annotations for the test libraries
        // we use.
        excludeTests = true

        extraJavacArgs = [
          "-AskipDefs=${skipDefCombinedRegex}",
          "-AskipUses=${skipUsesCombinedRegex}",
          "-AnoWarnMemoryConstraints",
          "-AsuppressWarnings=annotation.not.completed,keyfor",
        ]

        project.dependencies {
          checkerFramework("org.checkerframework:checker:$checkerframework_version")
        }
        project.configurations.all {
          it.exclude(group:"org.checkerframework", module:"jdk8")
        }
      }

      // Ban these dependencies from all configurations
      project.configurations.all {
        // guava-jdk5 brings in classes which conflict with guava
        exclude group: "com.google.guava", module: "guava-jdk5"
        // Ban the usage of the JDK tools as a library as this is system dependent
        exclude group: "jdk.tools", module: "jdk.tools"
        // protobuf-lite duplicates classes which conflict with protobuf-java
        exclude group: "com.google.protobuf", module: "protobuf-lite"
        // Exclude these test dependencies because they bundle other common
        // test libraries classes causing version conflicts. Users should rely
        // on using the yyy-core package instead of the yyy-all package.
        exclude group: "org.hamcrest", module: "hamcrest-all"
      }

      // Force usage of the libraries defined within our common set found in the root
      // build.gradle instead of using Gradles default dependency resolution mechanism
      // which chooses the latest version available.
      //
      // TODO: Figure out whether we should force all dependency conflict resolution
      // to occur in the "shadow" and "shadowTest" configurations.
      project.configurations.all { config ->
        // When running beam_Dependency_Check, resolutionStrategy should not be used; otherwise
        // gradle-versions-plugin does not report the latest versions of the dependencies.
        def startTasks = project.gradle.startParameter.taskNames
        def inDependencyUpdates = 'dependencyUpdates' in startTasks || 'runBeamDependencyCheck' in startTasks

        // The "errorprone" configuration controls the classpath used by errorprone static analysis, which
        // has different dependencies than our project.
        if (config.getName() != "errorprone" && !inDependencyUpdates) {
          config.resolutionStrategy {
            // Filtering versionless coordinates that depend on BOM. Beam project needs to set the
            // versions for only handful libraries when building the project (BEAM-9542).
            def librariesWithVersion = project.library.java.values().findAll { it.split(':').size() > 2 }
            force librariesWithVersion

            // hamcrest-core and hamcrest-library have been superseded by hamcrest.
            // We force their versions here to ensure that any resolved version provides
            // the same classes as hamcrest.
            force "org.hamcrest:hamcrest-core:$hamcrest_version"
            force "org.hamcrest:hamcrest-library:$hamcrest_version"
          }
        }
      }

      def jacocoExcludes = [
        '**/org/apache/beam/gradle/**',
        '**/org/apache/beam/model/**',
        '**/org/apache/beam/runners/dataflow/worker/windmill/**',
        '**/AutoValue_*'
      ]

      def jacocoEnabled = project.hasProperty('enableJacocoReport')
      if (jacocoEnabled) {
        project.tasks.withType(Test) {
          finalizedBy project.jacocoTestReport
        }
      }

      project.test {
        jacoco {
          excludes = jacocoExcludes
        }
      }

      project.jacocoTestReport {
        getClassDirectories().setFrom(project.files(
            project.fileTree(
            dir: project.getLayout().getBuildDirectory().dir("classes/java/main"),
            excludes: jacocoExcludes
            )
            ))
        getSourceDirectories().setFrom(
            project.files(project.sourceSets.main.allSource.srcDirs)
            )
        getExecutionData().setFrom(project.file(
            project.getLayout().getBuildDirectory().file("jacoco/test.exec")
            ))
        reports {
          html.required = true
          xml.required = true
          html.outputLocation = project.file(
              project.getLayout().getBuildDirectory().dir("jacoco/report")
              )
        }
      }

      if (configuration.shadowClosure) {
        // Ensure that tests are packaged and part of the artifact set.
        project.task('packageTests', type: Jar) {
          archiveClassifier = 'tests-unshaded'
          from project.sourceSets.test.output
        }
        project.artifacts.archives project.packageTests
      }

      // Note that these plugins specifically use the compileOnly and testCompileOnly
      // configurations because they are never required to be shaded or become a
      // dependency of the output.
      def compileOnlyAnnotationDeps = [
        "com.google.auto.service:auto-service-annotations:$autoservice_version",
        "com.google.auto.value:auto-value-annotations:$autovalue_version",
        "com.google.code.findbugs:jsr305:$jsr305_version",
        "com.google.j2objc:j2objc-annotations:3.0.0",
        // These dependencies are needed to avoid error-prone warnings on package-info.java files,
        // also to include the annotations to suppress warnings.
        //
        // spotbugs-annotations artifact is licensed under LGPL and cannot be included in the
        // Apache Beam distribution, but may be relied on during build.
        // See: https://www.apache.org/legal/resolved.html#prohibited
        // Special case for jsr305 (a transitive dependency of spotbugs-annotations):
        // sdks/java/core's FieldValueTypeInformation needs javax.annotations.Nullable at runtime.
        // Therefore, the java core module declares jsr305 dependency (BSD license) as "compile".
        // https://github.com/findbugsproject/findbugs/blob/master/findbugs/licenses/LICENSE-jsr305.txt
        "com.github.spotbugs:spotbugs-annotations:$spotbugs_version",
        "net.jcip:jcip-annotations:1.0",
        // This explicitly adds javax.annotation.Generated (SOURCE retention)
        // as a compile time dependency since Java 9+ no longer includes common
        // EE annotations: http://bugs.openjdk.java.net/browse/JDK-8152842. This
        // is required for grpc: http://github.com/grpc/grpc-java/issues/5343.
        //
        // javax.annotation is licensed under GPL 2.0 with Classpath Exception
        // and must not be included in the Apache Beam distribution, but may be
        // relied on during build.
        // See exception in: https://www.apache.org/legal/resolved.html#prohibited
        // License: https://github.com/javaee/javax.annotation/blob/1.3.2/LICENSE
        "javax.annotation:javax.annotation-api:1.3.2",
      ]

      project.dependencies {
        compileOnlyAnnotationDeps.each { dep ->
          compileOnly dep
          testCompileOnly dep
          annotationProcessor dep
          testAnnotationProcessor dep
        }

        // Add common annotation processors to all Java projects
        def annotationProcessorDeps = [
          "com.google.auto.value:auto-value:$autovalue_version",
          "com.google.auto.service:auto-service:$autoservice_version",
        ]

        annotationProcessorDeps.each { dep ->
          annotationProcessor dep
          testAnnotationProcessor dep
        }

        // This contains many improved annotations beyond javax.annotations for enhanced static checking
        // of the codebase. It is runtime so users can also take advantage of them. The annotations themselves
        // are MIT licensed (checkerframework is GPL and cannot be distributed)
        implementation "org.checkerframework:checker-qual:$checkerframework_version"
      }

      // Defines Targets for sonarqube analysis reporting.
      project.apply plugin: "org.sonarqube"

      // Configures a checkstyle plugin enforcing a set of rules and also allows for a set of
      // suppressions.
      project.apply plugin: 'checkstyle'
      project.checkstyle {
        configDirectory = project.rootProject.layout.projectDirectory.dir("sdks/java/build-tools/src/main/resources/beam/checkstyle")
        configFile = project.rootProject.layout.projectDirectory.file("sdks/java/build-tools/src/main/resources/beam/checkstyle/checkstyle.xml").asFile
        showViolations = true
        maxErrors = 0
        toolVersion = "8.23"
      }
      // CheckStyle can be removed from the 'check' task by passing -PdisableCheckStyle=true on the Gradle
      // command-line. This is useful for pre-commit which runs checkStyle separately.
      def disableCheckStyle = project.hasProperty('disableCheckStyle') &&
          project.disableCheckStyle == 'true'
      project.checkstyleMain.enabled = !disableCheckStyle
      project.checkstyleTest.enabled = !disableCheckStyle

      // Configures javadoc plugin and ensure check runs javadoc.
      project.tasks.withType(Javadoc) {
        options.encoding = 'UTF-8'
        options.addBooleanOption('Xdoclint:-missing', true)
      }
      project.check.dependsOn project.javadoc

      // Enables a plugin which can apply code formatting to source.
      project.apply plugin: "com.diffplug.spotless"
      // scan CVE
      project.apply plugin: "net.ossindex.audit"
      project.audit { rateLimitAsError = false }
      // Spotless can be removed from the 'check' task by passing -PdisableSpotlessCheck=true on the Gradle
      // command-line. This is useful for pre-commit which runs spotless separately.
      def disableSpotlessCheck = project.hasProperty('disableSpotlessCheck') &&
          project.disableSpotlessCheck == 'true'
      project.spotless {
        enforceCheck !disableSpotlessCheck
        java {
          licenseHeader javaLicenseHeader
          googleJavaFormat('1.7')
          target project.fileTree(project.projectDir) {
            include 'src/*/java/**/*.java'
            exclude '**/DefaultPackageTest.java'
          }
        }
      }

      // Enables a plugin which performs code analysis for common bugs.
      // This plugin is configured to only analyze the "main" source set.
      if (configuration.enableSpotbugs) {
        project.tasks.whenTaskAdded {task ->
          if(task.name.contains("spotbugsTest")) {
            task.enabled = false
          }
        }
        project.apply plugin: 'com.github.spotbugs'
        project.dependencies {
          spotbugs "com.github.spotbugs:spotbugs:$spotbugs_version"
          spotbugs "com.google.auto.value:auto-value:$autovalue_version"
          compileOnlyAnnotationDeps.each { dep -> spotbugs dep }
        }
        project.spotbugs {
          excludeFilter = project.rootProject.file('sdks/java/build-tools/src/main/resources/beam/spotbugs-filter.xml')
          jvmArgs = ['-Xmx12g']
        }
        project.tasks.withType(com.github.spotbugs.snom.SpotBugsTask) {
          reports {
            html.enabled = true
            xml.enabled = false
          }
        }
      }

      // Disregard unused but declared (test) compile only dependencies used
      // for common annotation classes used during compilation such as annotation
      // processing or post validation such as spotbugs.
      project.dependencies {
        compileOnlyAnnotationDeps.each { dep ->
          permitUnusedDeclared dep
          permitTestUnusedDeclared dep
        }
        permitUnusedDeclared "org.checkerframework:checker-qual:$checkerframework_version"
      }

      if (configuration.enableStrictDependencies) {
        project.tasks.analyzeClassesDependencies.enabled = true
        project.tasks.analyzeDependencies.enabled = true
        project.tasks.analyzeTestClassesDependencies.enabled = false
      } else {
        project.tasks.analyzeClassesDependencies.enabled = false
        project.tasks.analyzeTestClassesDependencies.enabled = false
        project.tasks.analyzeDependencies.enabled = false
      }

      // errorprone requires java9+ compiler. It can be used with Java8 but then sets a java9+ errorproneJavac.
      // However, the redirect ignores any task that forks and defines either a javaHome or an executable,
      // see https://github.com/tbroyer/gradle-errorprone-plugin#jdk-8-support
      // which means errorprone cannot run when gradle runs on Java11+ but serve `-testJavaVersion=8 -Pjava8Home` options
      if (!(project.findProperty('testJavaVersion') == '8')) {
        // Enable errorprone static analysis
        project.apply plugin: 'net.ltgt.errorprone'

        project.dependencies {
          errorprone("com.google.errorprone:error_prone_core:$errorprone_version")
          errorprone("jp.skypencil.errorprone.slf4j:errorprone-slf4j:0.1.2")
        }

        project.configurations.errorprone { resolutionStrategy.force "com.google.errorprone:error_prone_core:$errorprone_version" }

        project.tasks.withType(JavaCompile) {
          options.errorprone.disableWarningsInGeneratedCode = true
          options.errorprone.excludedPaths = '(.*/)?(build/generated-src|build/generated.*avro-java|build/generated)/.*'

          // Error Prone requires some packages to be exported/opened on Java versions that support modules,
          // i.e. Java 9 and up. The flags became mandatory in Java 17 with JEP-403.
          // The -J prefix is not needed if forkOptions.javaHome is unset,
          // see http://github.com/gradle/gradle/issues/22747
          if (options.forkOptions.javaHome == null) {
            options.fork = true
            options.forkOptions.jvmArgs += errorProneAddModuleOpts
          }

          // TODO(https://github.com/apache/beam/issues/20955): Enable errorprone checks
          options.errorprone.errorproneArgs.add("-Xep:AutoValueImmutableFields:OFF")
          options.errorprone.errorproneArgs.add("-Xep:AutoValueSubclassLeaked:OFF")
          options.errorprone.errorproneArgs.add("-Xep:BadImport:OFF")
          options.errorprone.errorproneArgs.add("-Xep:BadInstanceof:OFF")
          options.errorprone.errorproneArgs.add("-Xep:BigDecimalEquals:OFF")
          options.errorprone.errorproneArgs.add("-Xep:ComparableType:OFF")
          options.errorprone.errorproneArgs.add("-Xep:DoNotMockAutoValue:OFF")
          options.errorprone.errorproneArgs.add("-Xep:EmptyBlockTag:OFF")
          options.errorprone.errorproneArgs.add("-Xep:EmptyCatch:OFF")
          options.errorprone.errorproneArgs.add("-Xep:EqualsGetClass:OFF")
          options.errorprone.errorproneArgs.add("-Xep:EqualsUnsafeCast:OFF")
          options.errorprone.errorproneArgs.add("-Xep:EscapedEntity:OFF")
          options.errorprone.errorproneArgs.add("-Xep:ExtendsAutoValue:OFF")
          options.errorprone.errorproneArgs.add("-Xep:InlineFormatString:OFF")
          options.errorprone.errorproneArgs.add("-Xep:InlineMeSuggester:OFF")
          options.errorprone.errorproneArgs.add("-Xep:InvalidBlockTag:OFF")
          options.errorprone.errorproneArgs.add("-Xep:InvalidInlineTag:OFF")
          options.errorprone.errorproneArgs.add("-Xep:InvalidLink:OFF")
          options.errorprone.errorproneArgs.add("-Xep:InvalidParam:OFF")
          options.errorprone.errorproneArgs.add("-Xep:InvalidThrows:OFF")
          options.errorprone.errorproneArgs.add("-Xep:JavaTimeDefaultTimeZone:OFF")
          options.errorprone.errorproneArgs.add("-Xep:JavaUtilDate:OFF")
          options.errorprone.errorproneArgs.add("-Xep:JodaConstructors:OFF")
          options.errorprone.errorproneArgs.add("-Xep:MalformedInlineTag:OFF")
          options.errorprone.errorproneArgs.add("-Xep:MissingSummary:OFF")
          options.errorprone.errorproneArgs.add("-Xep:MixedMutabilityReturnType:OFF")
          options.errorprone.errorproneArgs.add("-Xep:PreferJavaTimeOverload:OFF")
          options.errorprone.errorproneArgs.add("-Xep:MutablePublicArray:OFF")
          options.errorprone.errorproneArgs.add("-Xep:NonCanonicalType:OFF")
          options.errorprone.errorproneArgs.add("-Xep:ProtectedMembersInFinalClass:OFF")
          options.errorprone.errorproneArgs.add("-Xep:Slf4jFormatShouldBeConst:OFF")
          options.errorprone.errorproneArgs.add("-Xep:Slf4jSignOnlyFormat:OFF")
          options.errorprone.errorproneArgs.add("-Xep:StaticAssignmentInConstructor:OFF")
          options.errorprone.errorproneArgs.add("-Xep:ThreadPriorityCheck:OFF")
          options.errorprone.errorproneArgs.add("-Xep:TimeUnitConversionChecker:OFF")
          options.errorprone.errorproneArgs.add("-Xep:UndefinedEquals:OFF")
          options.errorprone.errorproneArgs.add("-Xep:UnescapedEntity:OFF")
          options.errorprone.errorproneArgs.add("-Xep:UnnecessaryLambda:OFF")
          options.errorprone.errorproneArgs.add("-Xep:UnnecessaryMethodReference:OFF")
          options.errorprone.errorproneArgs.add("-Xep:UnnecessaryParentheses:OFF")
          options.errorprone.errorproneArgs.add("-Xep:UnrecognisedJavadocTag:OFF")
          options.errorprone.errorproneArgs.add("-Xep:UnsafeReflectiveConstructionCast:OFF")
          options.errorprone.errorproneArgs.add("-Xep:UseCorrectAssertInTests:OFF")

          // Sometimes a static logger is preferred, which is the convention
          // currently used in beam. See docs:
          // https://github.com/KengoTODA/findbugs-slf4j#slf4j_logger_should_be_non_static
          options.errorprone.errorproneArgs.add("-Xep:Slf4jLoggerShouldBeNonStatic:OFF")
        }
      }

      // Handle compile Java versions
      project.tasks.withType(JavaCompile).configureEach {
        // we configure the Java compiler to use UTF-8.
        options.encoding = "UTF-8"
        // If compiled on newer JDK, set byte code compatibility
        if (requireJavaVersion.compareTo(JavaVersion.current()) < 0) {
          def compatVersion = project.javaVersion == '1.8' ? '8' : project.javaVersion
          options.compilerArgs += ['--release', compatVersion]
          // TODO(https://github.com/apache/beam/issues/23901): Fix
          // optimizerOuterThis breakage
          options.compilerArgs += ['-XDoptimizeOuterThis=false']
        } else if (forkJavaVersion) {
          // If compiled on older SDK, compile with JDK configured with compatible javaXXHome
          setCompileAndRuntimeJavaVersion(options.compilerArgs, requireJavaVersion as String)
          project.ext.setJavaVerOptions(options, forkJavaVersion)
        }
        // As we want to add '-Xlint:-deprecation' we intentionally remove '-Xlint:deprecation' from compilerArgs here,
        // as intellij is adding this, see https://youtrack.jetbrains.com/issue/IDEA-196615
        options.compilerArgs -= [
          "-Xlint:deprecation",
        ]
        options.compilerArgs += ([
          '-parameters',
          '-Xlint:all',
          '-Werror'
        ]
        + (defaultLintSuppressions + configuration.disableLintWarnings).collect { "-Xlint:-${it}" })
      }

      if (forkJavaVersion) {
        project.tasks.withType(Javadoc) {
          executable = project.findProperty('java' + forkJavaVersion + 'Home') + '/bin/javadoc'
        }
      }

      project.tasks.withType(Jar).configureEach {
        preserveFileTimestamps(false)
      }

      // if specified test java version, modify the compile and runtime versions accordingly
      if (['8', '11', '17', '21'].contains(project.findProperty('testJavaVersion'))) {
        String ver = project.getProperty('testJavaVersion')
        def testJavaHome = project.getProperty("java${ver}Home")

        // redirect java compiler to specified version for compileTestJava only
        project.tasks.compileTestJava {
          setCompileAndRuntimeJavaVersion(options.compilerArgs, ver)
          project.ext.setJavaVerOptions(options, ver)
        }
        // redirect java runtime to specified version for running tests
        project.tasks.withType(Test).configureEach {
          useJUnit()
          executable = "${testJavaHome}/bin/java"
        }
      }

      if (configuration.shadowClosure) {
        // Enables a plugin which can perform shading of classes. See the general comments
        // above about dependency management for Java projects and how the shadow plugin
        // is expected to be used for the different Gradle configurations.
        //
        // TODO: Enforce all relocations are always performed to:
        // getJavaRelocatedPath(package_suffix) where package_suffix is something like "com.google.commmon"
        project.apply plugin: 'com.gradleup.shadow'

        // Create a new configuration 'shadowTest' like 'shadow' for the test scope
        project.configurations {
          shadow { description = "Dependencies for shaded source set 'main'" }
          implementation.extendsFrom shadow
          shadowTest {
            description = "Dependencies for shaded source set 'test'"
            extendsFrom shadow
          }
          testImplementation.extendsFrom shadowTest
        }

        // Ensure build task depends on shadowJar (required for shadow plugin >=4.0.4)
        project.tasks.named('build').configure {
          dependsOn project.tasks.named('shadowJar')
        }
      }

      project.jar {
        setAutomaticModuleNameHeader(configuration, project)

        zip64 true
        into("META-INF/") {
          from "${project.rootProject.projectDir}/LICENSE"
          from "${project.rootProject.projectDir}/NOTICE"
        }
      }

      // Always configure the shadowJar archiveClassifier and merge service files.
      if (configuration.shadowClosure) {
        // Only set the classifer on the unshaded classes if we are shading.
        project.jar { archiveClassifier = "unshaded" }

        project.shadowJar({
          archiveClassifier = null
          mergeServiceFiles()
          zip64 true
          into("META-INF/") {
            from "${project.rootProject.projectDir}/LICENSE"
            from "${project.rootProject.projectDir}/NOTICE"
          }
        } << configuration.shadowClosure)

        // Always configure the shadowTestJar archiveClassifier and merge service files.
        project.task('shadowTestJar', type: ShadowJar, {
          group = "Shadow"
          description = "Create a combined JAR of project and test dependencies"
          archiveClassifier = "tests"
          from project.sourceSets.test.output
          configurations = [
            project.configurations.testRuntimeMigration
          ]
          zip64 true
          exclude "META-INF/INDEX.LIST"
          exclude "META-INF/*.SF"
          exclude "META-INF/*.DSA"
          exclude "META-INF/*.RSA"
        } << configuration.shadowClosure)

        // Ensure that shaded jar and test-jar are part of their own configuration artifact sets
        project.artifacts.shadow project.shadowJar
        project.artifacts.shadowTest project.shadowTestJar

        if (configuration.testShadowJar) {
          // Use a configuration and dependency set which represents the execution classpath using shaded artifacts for tests.
          project.configurations { shadowTestRuntimeClasspath }

          project.dependencies {
            shadowTestRuntimeClasspath it.project(path: project.path, configuration: "shadowTest")
            shadowTestRuntimeClasspath it.project(path: project.path)
          }

          project.test { classpath = project.configurations.shadowTestRuntimeClasspath }
        }

        if (configuration.validateShadowJar) {
          def validateShadedJarDoesntLeakNonProjectClasses = project.tasks.register('validateShadedJarDoesntLeakNonProjectClasses') {
            dependsOn 'shadowJar'
            ext.outFile = project.file("${project.reportsDir}/${name}.out")
            inputs.files(project.configurations.shadow.artifacts.files)
                .withPropertyName("shadowArtifactsFiles")
                .withPathSensitivity(PathSensitivity.RELATIVE)
            outputs.files outFile
            doLast {
              project.configurations.shadow.artifacts.files.each {
                FileTree exposedClasses = project.zipTree(it).matching {
                  include "**/*.class"
                  // BEAM-5919: Exclude paths for Java 9 multi-release jars.
                  exclude "META-INF/versions/*/module-info.class"
                  configuration.shadowJarValidationExcludes.each {
                    exclude "$it"
                    exclude "META-INF/versions/*/$it"
                  }
                }
                outFile.text = exposedClasses.files
                if (exposedClasses.files) {
                  throw new GradleException("$it exposed classes outside of ${configuration.shadowJarValidationExcludes}: ${exposedClasses.files}")
                }
              }
            }
          }
          project.tasks.check.dependsOn validateShadedJarDoesntLeakNonProjectClasses
        }
      } else {
        project.tasks.register("testJar", Jar) {
          group = "Jar"
          description = "Create a JAR of test classes"
          archiveClassifier = "tests"
          from project.sourceSets.test.output
          zip64 true
          exclude "META-INF/INDEX.LIST"
          exclude "META-INF/*.SF"
          exclude "META-INF/*.DSA"
          exclude "META-INF/*.RSA"
        }
        project.artifacts.testRuntimeMigration project.testJar
      }

      if (configuration.enableJmh) {
        project.dependencies {
          runtimeOnly it.project(path: ":sdks:java:testing:test-utils")
          annotationProcessor "org.openjdk.jmh:jmh-generator-annprocess:$jmh_version"
          implementation project.library.java.jmh_core
        }

        project.compileJava {
          // Always exclude checkerframework on JMH generated code. It's slow,
          // and it often raises erroneous error because we don't have checker
          // annotations for generated code and test libraries.
          //
          // Consider re-enabling if we can get annotations for the generated
          // code and test libraries we use.
          checkerFramework {
            skipCheckerFramework = true
          }
        }

        project.tasks.register("jmh", JavaExec)  {
          dependsOn project.classes
          // Note: this will wrap the default JMH runner publishing results to InfluxDB
          mainClass = "org.apache.beam.sdk.testutils.jmh.Main"
          classpath = project.sourceSets.main.runtimeClasspath

          environment 'INFLUXDB_BASE_MEASUREMENT', 'java_jmh'

          // For a list of arguments, see
          // https://github.com/guozheng/jmh-tutorial/blob/master/README.md
          //
          // Enumerate available benchmarks and exit (uncomment below and disable other args)
          // args '-l'
          //
          // Enable connecting a debugger by disabling forking (uncomment below and disable other args)
          // Useful for debugging via an IDE such as Intellij
          // args '-f=0'
          // Specify -Pbenchmark=ProcessBundleBenchmark.testTinyBundle on the command
          // line to enable running a single benchmark.

          // Enable Google Cloud Profiler and upload the benchmarks to GCP.
          if (project.hasProperty("benchmark")) {
            args project.getProperty("benchmark")
            // Add JVM arguments allowing one to additionally use Google's Java Profiler
            // Agent: (see https://cloud.google.com/profiler/docs/profiling-java#installing-profiler for instructions on how to install)
            if (project.file("/opt/cprof/profiler_java_agent.so").exists()) {
              def gcpProject = project.findProperty('gcpProject') ?: 'apache-beam-testing'
              def userName = System.getProperty("user.name").toLowerCase().replaceAll(" ", "_")
              jvmArgs '-agentpath:/opt/cprof/profiler_java_agent.so=-cprof_service=' + userName + "_" + project.getProperty("benchmark").toLowerCase() + '_' + String.format('%1$tY%1$tm%1$td_%1$tH%1$tM%1$tS_%1$tL', System.currentTimeMillis()) + ',-cprof_project_id=' + gcpProject + ',-cprof_zone_name=us-central1-a'
            }
          } else {
            // We filter for only Apache Beam benchmarks to ensure that we aren't
            // running benchmarks that may have been packaged from another library
            // that ends up on the runtime classpath.
            args 'org.apache.beam'
          }
          // Reduce forks to 3
          args '-f=3'
          args '-foe=true'
        }

        // Single shot of JMH benchmarks ensures that they can execute.
        //
        // Note that these tests will fail on JVMs that JMH doesn't support.
        def jmhTest = project.tasks.register("jmhTest", JavaExec) {
          dependsOn project.classes
          // Note: this will just delegate to the default JMH runner, single shot times are not published to InfluxDB
          mainClass = "org.apache.beam.sdk.testutils.jmh.Main"
          classpath = project.sourceSets.main.runtimeClasspath

          // We filter for only Apache Beam benchmarks to ensure that we aren't
          // running benchmarks that may have been packaged from another library
          // that ends up on the runtime classpath.
          args 'org.apache.beam'
          args '-bm=ss'
          args '-i=1'
          args '-f=0'
          args '-wf=0'
          args '-foe=true'
          // Allow jmhTest to run concurrently with other jmhTest instances
          systemProperties(['jmh.ignoreLock' : 'true'])
        }
        project.check.dependsOn jmhTest
      }

      project.ext.includeInJavaBom = configuration.publish
      project.ext.exportJavadoc = configuration.exportJavadoc

      if (forkJavaVersion == '') {
        // project needs newer version and not served.
        // If not publishing ,disable the project. Otherwise, fail the build
        def msg = "project ${project.name} needs newer Java version to compile. Consider set -Pjava${project.javaVersion}Home"
        if (isRelease(project)) {
          throw new GradleException("Release enabled but " + msg + ".")
        } else {
          logger.config(msg + " if needed.")
          project.tasks.each {
            it.enabled = false
          }
        }
      }
      if ((isRelease(project) || project.hasProperty('publishing')) && configuration.publish) {
        project.apply plugin: "maven-publish"

        // plugin to support repository authentication via ~/.m2/settings.xml
        // https://github.com/mark-vieira/gradle-maven-settings-plugin/
        project.apply plugin: 'net.linguica.maven-settings'

        // Create a task which emulates the maven-archiver plugin in generating a
        // pom.properties file.
        def pomPropertiesFile = "${project.buildDir}/publications/mavenJava/pom.properties"
        project.task('generatePomPropertiesFileForMavenJavaPublication') {
          outputs.file "${pomPropertiesFile}"
          doLast {
            new File("${pomPropertiesFile}").text =
                """version=${project.version}
                       groupId=${project.mavenGroupId}
                       artifactId=${project.archivesBaseName}"""
          }
        }

        // Have the main artifact jar include both the generate pom.xml and its properties file
        // emulating the behavior of the maven-archiver plugin.
        project.(configuration.shadowClosure ? 'shadowJar' : 'jar') {
          def pomFile = "${project.buildDir}/publications/mavenJava/pom-default.xml"

          // Validate that the artifacts exist before copying them into the jar.
          doFirst {
            if (!project.file("${pomFile}").exists()) {
              throw new GradleException("Expected ${pomFile} to have been generated by the 'generatePomFileForMavenJavaPublication' task.")
            }
            if (!project.file("${pomPropertiesFile}").exists()) {
              throw new GradleException("Expected ${pomPropertiesFile} to have been generated by the 'generatePomPropertiesFileForMavenJavaPublication' task.")
            }
          }

          dependsOn 'generatePomFileForMavenJavaPublication'
          into("META-INF/maven/${project.mavenGroupId}/${project.archivesBaseName}") {
            from "${pomFile}"
            rename('.*', 'pom.xml')
          }

          dependsOn project.generatePomPropertiesFileForMavenJavaPublication
          into("META-INF/maven/${project.mavenGroupId}/${project.archivesBaseName}") { from "${pomPropertiesFile}" }
        }

        // Only build artifacts for archives if we are publishing
        if (configuration.shadowClosure) {
          project.artifacts.archives project.shadowJar
          project.artifacts.archives project.shadowTestJar
        } else {
          project.artifacts.archives project.jar
          project.artifacts.archives project.testJar
        }

        project.task('sourcesJar', type: Jar) {
          from project.sourceSets.main.allSource
          archiveClassifier = 'sources'
        }
        project.artifacts.archives project.sourcesJar

        project.task('testSourcesJar', type: Jar) {
          from project.sourceSets.test.allSource
          archiveClassifier = 'test-sources'
        }
        project.artifacts.archives project.testSourcesJar

        project.task('javadocJar', type: Jar, dependsOn: project.javadoc) {
          enabled = project.ext.exportJavadoc
          archiveClassifier = 'javadoc'
          from project.javadoc.destinationDir
        }
        project.artifacts.archives project.javadocJar

        project.publishing {
          repositories project.ext.repositories

          publications {
            mavenJava(MavenPublication) {
              if (configuration.shadowClosure) {
                artifact project.shadowJar
                artifact project.shadowTestJar
              } else {
                artifact project.jar
                artifact project.testJar
              }
              artifact project.sourcesJar
              artifact project.testSourcesJar
              artifact project.javadocJar

              artifactId = project.archivesBaseName
              groupId = project.mavenGroupId

              pom {
                name = project.description
                if (project.hasProperty("summary")) {
                  description = project.summary
                }
                url = "http://beam.apache.org"
                inceptionYear = "2016"
                licenses {
                  license {
                    name = "Apache License, Version 2.0"
                    url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                    distribution = "repo"
                  }
                }
                scm {
                  connection = "scm:git:https://gitbox.apache.org/repos/asf/beam.git"
                  developerConnection = "scm:git:https://gitbox.apache.org/repos/asf/beam.git"
                  url = "https://gitbox.apache.org/repos/asf?p=beam.git;a=summary"
                }
                issueManagement {
                  system = "github"
                  url = "https://github.com/apache/beam/issues"
                }
                mailingLists {
                  mailingList {
                    name = "Beam Dev"
                    subscribe = "dev-subscribe@beam.apache.org"
                    unsubscribe = "dev-unsubscribe@beam.apache.org"
                    post = "dev@beam.apache.org"
                    archive = "http://www.mail-archive.com/dev%beam.apache.org"
                  }
                  mailingList {
                    name = "Beam User"
                    subscribe = "user-subscribe@beam.apache.org"
                    unsubscribe = "user-unsubscribe@beam.apache.org"
                    post = "user@beam.apache.org"
                    archive = "http://www.mail-archive.com/user%beam.apache.org"
                  }
                  mailingList {
                    name = "Beam Commits"
                    subscribe = "commits-subscribe@beam.apache.org"
                    unsubscribe = "commits-unsubscribe@beam.apache.org"
                    post = "commits@beam.apache.org"
                    archive = "http://www.mail-archive.com/commits%beam.apache.org"
                  }
                }
                developers {
                  developer {
                    name = "The Apache Beam Team"
                    email = "dev@beam.apache.org"
                    url = "http://beam.apache.org"
                    organization = "Apache Software Foundation"
                    organizationUrl = "http://www.apache.org"
                  }
                }
              }

              pom.withXml {
                def root = asNode()

                // Add "repositories" section if it was configured
                if (configuration.mavenRepositories && configuration.mavenRepositories.size() > 0) {
                  def repositoriesNode = root.appendNode('repositories')
                  configuration.mavenRepositories.each {
                    def repositoryNode = repositoriesNode.appendNode('repository')
                    repositoryNode.appendNode('id', it.get('id'))
                    repositoryNode.appendNode('url', it.get('url'))
                  }
                }

                def dependenciesNode = root.appendNode('dependencies')

                // BOMs, declared with 'platform' or 'enforced-platform', appear in <dependencyManagement> section
                def boms = []

                def generateDependenciesFromConfiguration = { param ->
                  project.configurations."${param.configuration}".allDependencies.each {
                    String category = it.getAttributes().getAttribute(Category.CATEGORY_ATTRIBUTE)
                    if (Category.ENFORCED_PLATFORM == category || Category.REGULAR_PLATFORM == category) {
                      boms.add(it)
                      return
                    }

                    def dependencyNode = dependenciesNode.appendNode('dependency')
                    def appendClassifier = { dep ->
                      dep.artifacts.each { art ->
                        if (art.hasProperty('classifier')) {
                          dependencyNode.appendNode('classifier', art.classifier)
                        }
                      }
                    }

                    if (it instanceof ProjectDependency) {
                      dependencyNode.appendNode('groupId', it.getDependencyProject().mavenGroupId)
                      dependencyNode.appendNode('artifactId', it.getDependencyProject().archivesBaseName)
                      dependencyNode.appendNode('version', it.version)
                      dependencyNode.appendNode('scope', param.scope)
                      appendClassifier(it)
                    } else {
                      dependencyNode.appendNode('groupId', it.group)
                      dependencyNode.appendNode('artifactId', it.name)
                      if (it.version != null) { // bom-managed artifacts do not have their versions
                        dependencyNode.appendNode('version', it.version)
                      }
                      dependencyNode.appendNode('scope', param.scope)
                      appendClassifier(it)
                    }

                    // Start with any exclusions that were added via configuration exclude rules.
                    // Then add all the exclusions that are specific to the dependency (if any
                    // were declared). Finally build the node that represents all exclusions.
                    def exclusions = []
                    exclusions += project.configurations."${param.configuration}".excludeRules
                    if (it.hasProperty('excludeRules')) {
                      exclusions += it.excludeRules
                    }
                    if (!exclusions.empty) {
                      def exclusionsNode = dependencyNode.appendNode('exclusions')
                      exclusions.each { exclude ->
                        def exclusionNode = exclusionsNode.appendNode('exclusion')
                        exclusionNode.appendNode('groupId', exclude.group)
                        exclusionNode.appendNode('artifactId', exclude.module)
                      }
                    }
                  }
                }

                // TODO: Should we use the runtime scope instead of the compile scope
                // which forces all our consumers to declare what they consume?
                generateDependenciesFromConfiguration(
                    configuration: (configuration.shadowClosure ? 'shadow' : 'implementation'), scope: 'compile')
                generateDependenciesFromConfiguration(configuration: 'provided', scope: 'provided')

                if (!boms.isEmpty()) {
                  def dependencyManagementNode = root.appendNode('dependencyManagement')
                  def dependencyManagementDependencies = dependencyManagementNode.appendNode('dependencies')

                  // Resolve linkage error with guava jre vs android caused by Google Cloud libraries BOM
                  // https://github.com/GoogleCloudPlatform/cloud-opensource-java/wiki/The-Google-Cloud-Platform-Libraries-BOM#guava-versions--jre-or--android
                  def guavaDependencyNode = dependencyManagementDependencies.appendNode('dependency')
                  guavaDependencyNode.appendNode('groupId', 'com.google.guava')
                  guavaDependencyNode.appendNode('artifactId', 'guava')
                  guavaDependencyNode.appendNode('version', "$guava_version")

                  boms.each {
                    def dependencyNode = dependencyManagementDependencies.appendNode('dependency')
                    dependencyNode.appendNode('groupId', it.group)
                    dependencyNode.appendNode('artifactId', it.name)
                    dependencyNode.appendNode('version', it.version)
                    dependencyNode.appendNode('type', 'pom')
                    dependencyNode.appendNode('scope', 'import')
                  }
                }
                // NB: This must come after asNode() logic, as it seems asNode()
                // removes XML comments.
                // TODO: Load this from file?
                def elem = asElement()
                def hdr = elem.getOwnerDocument().createComment(
                    '''
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
''')
                elem.insertBefore(hdr, elem.getFirstChild())
              }
            }
          }
        }
        // Only sign artifacts if we are performing a release
        if (isRelease(project) && !project.hasProperty('noSigning')) {
          project.apply plugin: "signing"
          project.signing {
            useGpgCmd()
            sign project.publishing.publications
          }
        }
      }
    }
    def cleanUpTask = project.tasks.register('cleanUp') {
      dependsOn ':runners:google-cloud-dataflow-java:cleanUpDockerJavaImages'
    }

    // When applied in a module's build.gradle file, this closure provides task for running
    // IO integration tests.
    project.ext.enableJavaPerformanceTesting = {

      // Use the implicit it parameter of the closure to handle zero argument or one argument map calls.
      // See: http://groovy-lang.org/closures.html#implicit-it
      JavaPerformanceTestConfiguration configuration = it ? it as JavaPerformanceTestConfiguration : new JavaPerformanceTestConfiguration()

      // Task for running integration tests
      def itTask = project.task('integrationTest', type: Test) {

        // Disable Gradle cache (it should not be used because the IT's won't run).
        outputs.upToDateWhen { false }

        include "**/*IT.class"

        def pipelineOptionsString = configuration.integrationTestPipelineOptions
        def pipelineOptionsStringFormatted
        def allOptionsList

        if(pipelineOptionsString) {
          allOptionsList = (new JsonSlurper()).parseText(pipelineOptionsString)
        }

        if (pipelineOptionsString && configuration.runner?.equalsIgnoreCase('dataflow')) {
          if (pipelineOptionsString.contains('use_runner_v2')) {
            dependsOn ':runners:google-cloud-dataflow-java:buildAndPushDockerJavaContainer'
          }
        }

        // We construct the pipeline options during task execution time in order to get dockerJavaImageName.
        doFirst {
          if (pipelineOptionsString && configuration.runner?.equalsIgnoreCase('dataflow')) {
            def dataflowRegion = project.findProperty('dataflowRegion') ?: 'us-central1'
            if (pipelineOptionsString.contains('use_runner_v2')) {
              def dockerJavaImageName = project.project(':runners:google-cloud-dataflow-java').ext.dockerJavaImageName
              allOptionsList.addAll([
                "--sdkContainerImage=${dockerJavaImageName}",
                "--region=${dataflowRegion}"
              ])
            } else {
              project.evaluationDependsOn(":runners:google-cloud-dataflow-java:worker")
              def dataflowWorkerJar = project.findProperty('dataflowWorkerJar') ?:
                  project.project(":runners:google-cloud-dataflow-java:worker").shadowJar.archivePath
              allOptionsList.addAll([
                // Keep as legacy flag to ensure via test this flag works for
                // legacy pipeline.
                '--workerHarnessContainerImage=',
                "--dataflowWorkerJar=${dataflowWorkerJar}",
                "--region=${dataflowRegion}"
              ])
            }
          }

          // Windows handles quotation marks differently
          if (pipelineOptionsString && System.properties['os.name'].toLowerCase().contains('windows')) {
            def allOptionsListFormatted = allOptionsList.collect { "\"$it\"" }
            pipelineOptionsStringFormatted = JsonOutput.toJson(allOptionsListFormatted)
          } else if (pipelineOptionsString) {
            pipelineOptionsStringFormatted = JsonOutput.toJson(allOptionsList)
          }

          systemProperties.beamTestPipelineOptions = pipelineOptionsStringFormatted ?: pipelineOptionsString
        }
      }
      project.afterEvaluate {
        // Ensure all tasks which use published docker images run before they are cleaned up
        project.tasks.each { t ->
          if (t.dependsOn.contains(":runners:google-cloud-dataflow-java:buildAndPushDockerJavaContainer")) {
            t.finalizedBy cleanUpTask
          }
        }
      }
    }

    // When applied in a module's build.gradle file, this closure adds task providing
    // additional dependencies that might be needed while running integration tests.
    project.ext.provideIntegrationTestingDependencies = {

      // Use the implicit it parameter of the closure to handle zero argument or one argument map calls.
      // See: http://groovy-lang.org/closures.html#implicit-it
      JavaPerformanceTestConfiguration configuration = it ? it as JavaPerformanceTestConfiguration : new JavaPerformanceTestConfiguration()

      project.dependencies {
        def runner = configuration.runner
        def filesystem = configuration.filesystem

        /* include dependencies required by runners */
        //if (runner?.contains('dataflow')) {
        if (runner?.equalsIgnoreCase('dataflow')) {
          testRuntimeOnly it.project(path: ":runners:google-cloud-dataflow-java", configuration: "testRuntimeMigration")
          testRuntimeOnly it.project(path: ":runners:google-cloud-dataflow-java:worker", configuration: 'shadow')
        }

        if (runner?.equalsIgnoreCase('direct')) {
          testRuntimeOnly it.project(path: ":runners:direct-java", configuration: 'shadowTest')
        }

        if (runner?.equalsIgnoreCase('flink')) {
          testRuntimeOnly it.project(path: ":runners:flink:${project.ext.latestFlinkVersion}", configuration: "testRuntimeMigration")
        }

        if (runner?.equalsIgnoreCase('spark')) {
          testRuntimeOnly it.project(path: ":runners:spark:3", configuration: "testRuntimeMigration")

          // Testing the Spark runner causes a StackOverflowError if slf4j-jdk14 is on the classpath
          project.configurations.testRuntimeClasspath {
            exclude group: "org.slf4j", module: "slf4j-jdk14"
          }
        }

        /* include dependencies required by filesystems */
        if (filesystem?.equalsIgnoreCase('hdfs')) {
          testRuntimeOnly it.project(path: ":sdks:java:io:hadoop-file-system", configuration: "testRuntimeMigration")
          testRuntimeOnly project.library.java.hadoop_client
        }

        /* include dependencies required by AWS S3 */
        if (filesystem?.equalsIgnoreCase('s3')) {
          testRuntimeOnly it.project(path: ":sdks:java:io:amazon-web-services2", configuration: "testRuntimeMigration")
        }
      }
      project.task('packageIntegrationTests', type: Jar)
    }

    /** ***********************************************************************************************/

    project.ext.applyGoNature = {
      // Define common lifecycle tasks and artifact types
      project.apply plugin: 'base'

      // For some reason base doesn't define a test task  so we define it below and make
      // check depend on it. This makes the Go project similar to the task layout like
      // Java projects, see https://docs.gradle.org/4.2.1/userguide/img/javaPluginTasks.png
      if (project.tasks.findByName('test') == null) {
        project.task('test') {}
      }
      project.check.dependsOn project.test

      def goRootDir = "${project.rootDir}/sdks/go"

      // This sets the whole project Go version.
      // The latest stable Go version can be checked at https://go.dev/dl/
      project.ext.goVersion = "go1.24.4"

      // Minor TODO: Figure out if we can pull out the GOCMD env variable after goPrepare script
      // completion, and avoid this GOBIN substitution.
      project.ext.goCmd = "${goRootDir}/run_with_go_version.sh --gocmd GOBIN/${project.ext.goVersion}"

      def goPrepare = project.tasks.register("goPrepare") {
        description "Prepare ${project.ext.goVersion} for builds and tests."
        project.exec {
          executable 'sh'
          args '-c', "${goRootDir}/prepare_go_version.sh --version ${project.ext.goVersion}"
        }
      }

      def goBuild = project.tasks.register("goBuild") {
        dependsOn goPrepare
        ext.goTargets = './...'
        ext.outputLocation = './build/bin/${GOOS}_${GOARCH}/'
        doLast {
          for (goarch in project.containerArchitectures()) {
            project.exec {
              // Set these so the substitutions work.
              // May cause issues for the folks running gradle commands on other architectures
              // and operating systems.
              environment "GOOS", "linux"
              environment "GOARCH", goarch

              executable 'sh'
              args '-c', "${project.ext.goCmd} build -o "+ ext.outputLocation + ' ' + ext.goTargets
            }
          }
        }
      }

      project.tasks.register("goTest") {
        dependsOn goBuild
        doLast {
          project.exec {
            executable 'sh'
            args '-c', "${project.ext.goCmd} test ./..."
          }
        }
      }
    }

    /** ***********************************************************************************************/

    project.ext.applyDockerNature = {
      project.apply plugin: BeamDockerPlugin
      project.docker { noCache true }
      project.tasks.create(name: "copyLicenses", type: Copy) {
        from "${project.rootProject.projectDir}/LICENSE"
        from "${project.rootProject.projectDir}/LICENSE.python"
        from "${project.rootProject.projectDir}/NOTICE"
        into "build/target"
      }
      project.tasks.dockerPrepare.dependsOn project.tasks.copyLicenses
    }

    project.ext.applyDockerRunNature = {
      project.apply plugin: BeamDockerRunPlugin
    }
    /** ***********************************************************************************************/

    project.ext.applyGroovyNature = {
      project.apply plugin: "groovy"

      project.apply plugin: "com.diffplug.spotless"
      def disableSpotlessCheck = project.hasProperty('disableSpotlessCheck') &&
          project.disableSpotlessCheck == 'true'
      project.spotless {
        enforceCheck !disableSpotlessCheck
        def grEclipseConfig = project.project(":").file("buildSrc/greclipse.properties")
        groovy {
          greclipse().configFile(grEclipseConfig)
          target project.fileTree(project.projectDir) { include '**/*.groovy' }
        }
        groovyGradle { greclipse().configFile(grEclipseConfig) }
      }
      // Workaround to fix spotless groovy and groovyGradle tasks use the same intermediate dir,
      // until Beam no longer build on Java8 and can upgrade spotless plugin.
      project.tasks.spotlessGroovy.mustRunAfter project.tasks.spotlessGroovyGradle
    }

    // containerImageName returns a configurable container image name, by default a
    // development image at docker.io (see sdks/CONTAINERS.md):
    //
    //    format: apache/beam_$NAME_sdk:latest
    //    ie: apache/beam_python3.12_sdk:latest apache/beam_java21_sdk:latest apache/beam_go_sdk:latest
    //
    // Both the root and tag can be defined using properties or explicitly provided.
    project.ext.containerImageName = {
      // Use the implicit it parameter of the closure to handle zero argument or one argument map calls.
      ContainerImageNameConfiguration configuration = it ? it as ContainerImageNameConfiguration : new ContainerImageNameConfiguration()

      if (configuration.root == null) {
        if (project.rootProject.hasProperty(["docker-repository-root"])) {
          configuration.root = project.rootProject["docker-repository-root"]
        } else {
          configuration.root = "${System.properties["user.name"]}-docker-apache.bintray.io/beam"
        }
      }
      if (configuration.tag == null) {
        if (project.rootProject.hasProperty(["docker-tag"])) {
          configuration.tag = project.rootProject["docker-tag"]
        } else {
          configuration.tag = 'latest'
        }
      }
      return "${configuration.root}/${configuration.name}:${configuration.tag}"
    }

    project.ext.containerImageTags = {
      String[] tags
      if (project.rootProject.hasProperty(["docker-tag-list"])) {
        tags = project.rootProject["docker-tag-list"].split(',')
      } else {
        tags = [
          project.rootProject.hasProperty(["docker-tag"]) ?
          project.rootProject["docker-tag"] : project.sdk_version
        ]
      }
      return tags
    }

    /** ***********************************************************************************************/

    // applyGrpcNature should only be applied to projects who wish to use
    // unvendored gRPC / protobuf dependencies.
    project.ext.applyGrpcNature = {
      project.apply plugin: "com.google.protobuf"
      project.protobuf {
        protoc {
          // The artifact spec for the Protobuf Compiler
          artifact = "com.google.protobuf:protoc:$protobuf_version" }

        // Configure the codegen plugins
        plugins {
          // An artifact spec for a protoc plugin, with "grpc" as
          // the identifier, which can be referred to in the "plugins"
          // container of the "generateProtoTasks" closure.
          grpc { artifact = "io.grpc:protoc-gen-grpc-java:$grpc_version" }
        }

        generateProtoTasks {
          ofSourceSet("main")*.plugins {
            // Apply the "grpc" plugin whose spec is defined above, without
            // options.  Note the braces cannot be omitted, otherwise the
            // plugin will not be added. This is because of the implicit way
            // NamedDomainObjectContainer binds the methods.
            grpc {}
          }
        }
      }

      def generatedProtoMainJavaDir = "${project.buildDir}/generated/source/proto/main/java"
      def generatedProtoTestJavaDir = "${project.buildDir}/generated/source/proto/test/java"
      def generatedGrpcMainJavaDir = "${project.buildDir}/generated/source/proto/main/grpc"
      def generatedGrpcTestJavaDir = "${project.buildDir}/generated/source/proto/test/grpc"
      project.idea {
        module {
          sourceDirs += project.file(generatedProtoMainJavaDir)
          generatedSourceDirs += project.file(generatedProtoMainJavaDir)

          testSourceDirs += project.file(generatedProtoTestJavaDir)
          generatedSourceDirs += project.file(generatedProtoTestJavaDir)

          sourceDirs += project.file(generatedGrpcMainJavaDir)
          generatedSourceDirs += project.file(generatedGrpcMainJavaDir)

          testSourceDirs += project.file(generatedGrpcTestJavaDir)
          generatedSourceDirs += project.file(generatedGrpcTestJavaDir)
        }
      }
    }

    /** ***********************************************************************************************/

    // applyPortabilityNature should only be applied to projects that want to use
    // vendored gRPC / protobuf dependencies.
    project.ext.applyPortabilityNature = {
      PortabilityNatureConfiguration configuration = it ? it as PortabilityNatureConfiguration : new PortabilityNatureConfiguration()

      if (configuration.archivesBaseName) {
        project.archivesBaseName = configuration.archivesBaseName
      }

      project.ext.applyJavaNature(
          enableStrictDependencies: false,
          exportJavadoc: false,
          enableSpotbugs: false,
          publish: configuration.publish,
          generatedClassPatterns: configuration.generatedClassPatterns,
          archivesBaseName: configuration.archivesBaseName,
          automaticModuleName: configuration.automaticModuleName,
          shadowJarValidationExcludes: it.shadowJarValidationExcludes,
          shadowClosure: GrpcVendoring_1_69_0.shadowClosure() << {
            // We perform all the code relocations but don't include
            // any of the actual dependencies since they will be supplied
            // by org.apache.beam:beam-vendor-grpc-v1p69p0
            dependencies {
              include(dependency { return false })
            }
          })

      // Don't force modules here because we don't want to take the shared declarations in build_rules.gradle
      // because we would like to have the freedom to choose which versions of dependencies we
      // are using for the portability APIs separate from what is being used inside other modules such as GCP.
      project.configurations.all { config ->
        config.resolutionStrategy { forcedModules = []}
      }

      project.apply plugin: "com.google.protobuf"
      project.protobuf {
        protoc {
          // The artifact spec for the Protobuf Compiler
          artifact = "com.google.protobuf:protoc:${GrpcVendoring_1_69_0.protobuf_version}" }

        // Configure the codegen plugins
        plugins {
          // An artifact spec for a protoc plugin, with "grpc" as
          // the identifier, which can be referred to in the "plugins"
          // container of the "generateProtoTasks" closure.
          grpc { artifact = "io.grpc:protoc-gen-grpc-java:${GrpcVendoring_1_69_0.grpc_version}" }
        }

        generateProtoTasks {
          ofSourceSet("main")*.plugins {
            // Apply the "grpc" plugin whose spec is defined above, without
            // options.  Note the braces cannot be omitted, otherwise the
            // plugin will not be added. This is because of the implicit way
            // NamedDomainObjectContainer binds the methods.
            grpc { }
          }
        }
      }

      project.dependencies GrpcVendoring_1_69_0.dependenciesClosure() << { shadow project.ext.library.java.vendored_grpc_1_69_0 }
    }

    /** ***********************************************************************************************/

    // TODO: Decide whether this should be inlined into the one project that relies on it
    // or be left here.
    project.ext.applyAvroNature = {
      project.apply plugin: "com.github.davidmc24.gradle.plugin.avro"

      // add dependency BeamModulePlugin defined custom tasks
      // they are defined only when certain flags are provided (e.g. -Prelease; -Ppublishing, etc)
      def sourcesJar = project.tasks.findByName('sourcesJar')
      if (sourcesJar != null) {
        sourcesJar.dependsOn project.tasks.getByName('generateAvroJava')
      }
      def testSourcesJar = project.tasks.findByName('testSourcesJar')
      if (testSourcesJar != null) {
        testSourcesJar.dependsOn project.tasks.getByName('generateTestAvroJava')
      }
    }

    project.ext.applyAntlrNature = {
      project.apply plugin: 'antlr'
      project.idea {
        module {
          // mark antlrs output folders as generated
          generatedSourceDirs += project.generateGrammarSource.outputDirectory
          generatedSourceDirs += project.generateTestGrammarSource.outputDirectory
        }
      }

      // add dependency BeamModulePlugin defined custom tasks
      // they are defined only when certain flags are provided (e.g. -Prelease; -Ppublishing, etc)
      def sourcesJar = project.tasks.findByName('sourcesJar')
      if (sourcesJar != null) {
        sourcesJar.mustRunAfter project.tasks.getByName('generateGrammarSource')
      }
      def testSourcesJar = project.tasks.findByName('testSourcesJar')
      if (testSourcesJar != null) {
        testSourcesJar.dependsOn project.tasks.getByName('generateTestGrammarSource')
      }
    }

    // Creates a task to run the quickstart for a runner.
    // Releases version and URL, can be overriden for a RC release with
    // ./gradlew :release:runJavaExamplesValidationTask -Pver=2.3.0 -Prepourl=https://repository.apache.org/content/repositories/orgapachebeam-1027
    project.ext.createJavaExamplesArchetypeValidationTask = {
      JavaExamplesArchetypeValidationConfiguration config = it as JavaExamplesArchetypeValidationConfiguration

      def taskName = "run${config.type}Java${config.runner}"
      def releaseVersion = project.findProperty('ver') ?: project.version
      def releaseRepo = project.findProperty('repourl') ?: 'https://repository.apache.org/content/repositories/snapshots'
      // shared maven local path for maven archetype projects
      def sharedMavenLocal = project.findProperty('mavenLocalPath') ?: ''
      def argsNeeded = [
        "--ver=${releaseVersion}",
        "--repourl=${releaseRepo}"
      ]
      if (config.gcpProject) {
        argsNeeded.add("--gcpProject=${config.gcpProject}")
      }
      if (config.gcpRegion) {
        argsNeeded.add("--gcpRegion=${config.gcpRegion}")
      }
      if (config.gcsBucket) {
        argsNeeded.add("--gcsBucket=${config.gcsBucket}/${randomUUID().toString()}")
      }
      if (config.bqDataset) {
        argsNeeded.add("--bqDataset=${config.bqDataset}")
      }
      if (config.pubsubTopic) {
        argsNeeded.add("--pubsubTopic=${config.pubsubTopic}")
      }
      if (sharedMavenLocal) {
        argsNeeded.add("--mavenLocalPath=${sharedMavenLocal}")
      }
      project.evaluationDependsOn(':release')
      project.task(taskName, dependsOn: ':release:classes', type: JavaExec) {
        group = "Verification"
        description = "Run the Beam ${config.type} with the ${config.runner} runner"
        mainClass = "${config.type}-java-${config.runner}".toLowerCase()
        classpath = project.project(':release').sourceSets.main.runtimeClasspath
        args argsNeeded
      }
    }


    /** ***********************************************************************************************/

    // Method to create the PortableValidatesRunnerTask.
    // The method takes PortableValidatesRunnerConfiguration as parameter.
    project.ext.createPortableValidatesRunnerTask = {
      /*
       * We need to rely on manually specifying these evaluationDependsOn to ensure that
       * the following projects are evaluated before we evaluate this project. This is because
       * we are attempting to reference the "sourceSets.test.output" directly.
       */
      project.evaluationDependsOn(":sdks:java:core")
      project.evaluationDependsOn(":runners:core-java")
      def config = it ? it as PortableValidatesRunnerConfiguration : new PortableValidatesRunnerConfiguration()

      def name = config.name
      def beamTestPipelineOptions = [
        "--runner=org.apache.beam.runners.portability.testing.TestPortableRunner",
        "--jobServerDriver=${config.jobServerDriver}",
        "--environmentCacheMillis=10000",
        "--experiments=beam_fn_api",
      ]
      beamTestPipelineOptions.addAll(config.pipelineOpts)
      if (config.environment == PortableValidatesRunnerConfiguration.Environment.EMBEDDED) {
        beamTestPipelineOptions += "--defaultEnvironmentType=EMBEDDED"
      }
      if (config.jobServerConfig) {
        beamTestPipelineOptions.add("--jobServerConfig=${config.jobServerConfig}")
      }
      config.systemProperties.put("beamTestPipelineOptions", JsonOutput.toJson(beamTestPipelineOptions))
      project.tasks.register(name, Test) {
        group = "Verification"
        description = "Validates the PortableRunner with JobServer ${config.jobServerDriver}"
        systemProperties config.systemProperties
        classpath = config.testClasspathConfiguration
        testClassesDirs = project.files(project.project(":sdks:java:core").sourceSets.test.output.classesDirs, project.project(":runners:core-java").sourceSets.test.output.classesDirs)
        maxParallelForks config.numParallelTests
        useJUnit(config.testCategories)
        filter(config.testFilter)
        // increase maxHeapSize as this is directly correlated to direct memory,
        // see https://issues.apache.org/jira/browse/BEAM-6698
        maxHeapSize = '4g'
        if (config.environment == PortableValidatesRunnerConfiguration.Environment.DOCKER) {
          String ver = project.findProperty('testJavaVersion')
          def javaContainerSuffix = getSupportedJavaVersion(ver)
          dependsOn ":sdks:java:container:${javaContainerSuffix}:docker"
        }
      }
    }

    /** ***********************************************************************************************/
    // Method to create the createCrossLanguageUsingJavaExpansionTask.
    // The method takes CrossLanguageUsingJavaExpansionConfiguration as parameter.
    // This method creates a task that runs Python SDK test-suites that use external Java transforms
    project.ext.createCrossLanguageUsingJavaExpansionTask = {
      // This task won't work if the python build file doesn't exist.
      if (!project.project(":sdks:python").buildFile.exists()) {
        System.err.println 'Python build file not found. Skipping createCrossLanguageUsingJavaExpansionTask.'
        return
      }
      def config = it ? it as CrossLanguageUsingJavaExpansionConfiguration : new CrossLanguageUsingJavaExpansionConfiguration()

      project.evaluationDependsOn(":sdks:python")
      for (path in config.expansionProjectPaths) {
        project.evaluationDependsOn(path)
      }
      project.evaluationDependsOn(":sdks:java:extensions:python")

      def pythonDir = project.project(":sdks:python").projectDir
      def usesDataflowRunner = config.pythonPipelineOptions.contains("--runner=TestDataflowRunner") || config.pythonPipelineOptions.contains("--runner=DataflowRunner")
      def javaContainerSuffix = getSupportedJavaVersion()

      // Sets up, collects, and runs Python pipeline tests
      project.tasks.register(config.name+"PythonUsingJava") {
        group = "Verification"
        description = "Runs Python SDK pipeline tests that use a Java expansion service"
        // Each expansion service we use needs to be built before running these tests
        // The built jars will be started up automatically using the BeamJarExpansionService utility
        for (path in config.expansionProjectPaths) {
          dependsOn project.project(path).shadowJar.getPath()
        }
        dependsOn ":sdks:java:container:$javaContainerSuffix:docker"
        dependsOn "installGcpTest"
        if (usesDataflowRunner) {
          dependsOn ":sdks:python:test-suites:dataflow:py${project.ext.pythonVersion.replace('.', '')}:initializeForDataflowJob"
        }
        doLast {
          def beamPythonTestPipelineOptions = [
            "pipeline_opts": config.pythonPipelineOptions + (usesDataflowRunner ? [
              "--sdk_location=${project.ext.sdkLocation}"]
            : []),
            "test_opts": config.pytestOptions,
            "suite": config.name,
            "collect": config.collectMarker,
          ]
          def cmdArgs = project.project(':sdks:python').mapToArgString(beamPythonTestPipelineOptions)

          project.exec {
            // environment variable to indicate that jars have been built
            environment "EXPANSION_JARS", config.expansionProjectPaths
            String additionalDependencyCmd = ""
            if (config.additionalDeps != null && !config.additionalDeps.isEmpty()){
              additionalDependencyCmd = "&& pip install ${config.additionalDeps.join(' ')} "
            }
            executable 'sh'
            args '-c', ". ${project.ext.envdir}/bin/activate " +
                additionalDependencyCmd +
                "&& cd $pythonDir && ./scripts/run_integration_test.sh $cmdArgs"
          }
        }
      }
    }

    /** ***********************************************************************************************/

    // Method to create the crossLanguageValidatesRunnerTask.
    // The method takes crossLanguageValidatesRunnerConfiguration as parameter.
    project.ext.createCrossLanguageValidatesRunnerTask = {
      // This task won't work if the python build file doesn't exist.
      if (!project.project(":sdks:python").buildFile.exists()) {
        System.err.println 'Python build file not found. Skipping createCrossLanguageValidatesRunnerTask.'
        return
      }
      def config = it ? it as CrossLanguageValidatesRunnerConfiguration : new CrossLanguageValidatesRunnerConfiguration()

      project.evaluationDependsOn(":sdks:python")
      project.evaluationDependsOn(":sdks:java:testing:expansion-service")
      project.evaluationDependsOn(":sdks:java:core")
      project.evaluationDependsOn(":sdks:java:extensions:python")
      project.evaluationDependsOn(":sdks:go:test")

      // Task for launching expansion services
      def envDir = project.project(":sdks:python").envdir
      def pythonDir = project.project(":sdks:python").projectDir
      def javaPort = getRandomPort()
      def pythonPort = getRandomPort()
      def expansionJar = project.project(':sdks:java:testing:expansion-service').buildTestExpansionServiceJar.archivePath
      def javaClassLookupAllowlistFile = project.project(":sdks:java:testing:expansion-service").projectDir.getPath() + "/src/test/resources/test_expansion_service_allowlist.yaml"
      def expansionServiceOpts = [
        "group_id": project.name,
        "java_expansion_service_jar": expansionJar,
        "java_port": javaPort,
        "java_expansion_service_allowlist_file": javaClassLookupAllowlistFile,
        "python_expansion_service_allowlist_glob": "\\*",
        "python_virtualenv_dir": envDir,
        "python_expansion_service_module": "apache_beam.runners.portability.expansion_service_test",
        "python_port": pythonPort
      ]
      def serviceArgs = project.project(':sdks:python').mapToArgString(expansionServiceOpts)
      def pythonContainerSuffix = project.project(':sdks:python').pythonVersion.replace('.', '')
      def javaContainerSuffix = getSupportedJavaVersion()
      def setupTask = project.tasks.register(config.name+"Setup", Exec) {
        dependsOn ':sdks:java:container:'+javaContainerSuffix+':docker'
        dependsOn ':sdks:python:container:py'+pythonContainerSuffix+':docker'
        dependsOn ':sdks:java:testing:expansion-service:buildTestExpansionServiceJar'
        dependsOn ":sdks:python:installGcpTest"
        // setup test env
        executable 'sh'
        args '-c', "$pythonDir/scripts/run_expansion_services.sh stop --group_id ${project.name} && $pythonDir/scripts/run_expansion_services.sh start $serviceArgs"
      }

      def mainTask = project.tasks.register(config.name) {
        group = "Verification"
        description = "Validates cross-language capability of runner"
      }

      def cleanupTask = project.tasks.register(config.name+'Cleanup', Exec) {
        // teardown test env
        executable 'sh'
        args '-c', "$pythonDir/scripts/run_expansion_services.sh stop --group_id ${project.name}"
      }
      setupTask.configure {finalizedBy cleanupTask}
      config.startJobServer.configure {finalizedBy config.cleanupJobServer}

      def sdkLocationOpt = []
      if (config.needsSdkLocation) {
        setupTask.configure {dependsOn ':sdks:python:sdist'}
        sdkLocationOpt = [
          "--sdk_location=${pythonDir}/build/apache-beam.tar.gz"
        ]
      }

      String testJavaVersion = project.findProperty('testJavaVersion')
      String testJavaHome = null
      if (testJavaVersion) {
        testJavaHome = project.findProperty("java${testJavaVersion}Home")
      }

      ['Java': javaPort, 'Python': pythonPort].each { sdk, port ->
        // Task for running testcases in Java SDK
        def javaTask = project.tasks.register(config.name+"JavaUsing"+sdk, Test) {
          group = "Verification"
          description = "Validates runner for cross-language capability of using ${sdk} transforms from Java SDK"
          systemProperty "beamTestPipelineOptions", JsonOutput.toJson(config.javaPipelineOptions)
          systemProperty "expansionJar", expansionJar
          systemProperty "expansionPort", port
          systemProperty "semiPersistDir", config.semiPersistDir
          classpath = config.classpath + project.files(
              project.project(":sdks:java:core").sourceSets.test.runtimeClasspath,
              project.project(":sdks:java:extensions:python").sourceSets.test.runtimeClasspath
              )
          testClassesDirs = project.files(
              project.project(":sdks:java:core").sourceSets.test.output.classesDirs,
              project.project(":sdks:java:extensions:python").sourceSets.test.output.classesDirs
              )
          maxParallelForks config.numParallelTests
          if (sdk == "Java") {
            useJUnit{ includeCategories 'org.apache.beam.sdk.testing.UsesJavaExpansionService' }
          } else if (sdk == "Python") {
            useJUnit{ includeCategories 'org.apache.beam.sdk.testing.UsesPythonExpansionService' }
          } else {
            throw new GradleException("unsupported expansion service for Java validate runner tests.")
          }
          if (testJavaHome) {
            executable = "${testJavaHome}/bin/java"
          }
          // increase maxHeapSize as this is directly correlated to direct memory,
          // see https://issues.apache.org/jira/browse/BEAM-6698
          maxHeapSize = '4g'
          dependsOn setupTask
          dependsOn config.startJobServer
        }
        if (sdk != "Java") {
          mainTask.configure {dependsOn javaTask}
        }
        cleanupTask.configure {mustRunAfter javaTask}
        config.cleanupJobServer.configure {mustRunAfter javaTask}

        // Task for running testcases in Python SDK
        def beamPythonTestPipelineOptions = [
          "pipeline_opts": config.pythonPipelineOptions + sdkLocationOpt,
          "test_opts": config.pytestOptions,
          "suite": "xlangValidateRunner",
        ]
        if (sdk == "Java") {
          beamPythonTestPipelineOptions["collect"] = "uses_java_expansion_service"
        } else if (sdk == "Python") {
          beamPythonTestPipelineOptions["collect"] = "uses_python_expansion_service"
        } else {
          throw new GradleException("unsupported expansion service for Python validate runner tests.")
        }
        def cmdArgs = project.project(':sdks:python').mapToArgString(beamPythonTestPipelineOptions)
        def pythonTask = project.tasks.register(config.name+"PythonUsing"+sdk, Exec) {
          group = "Verification"
          description = "Validates runner for cross-language capability of using ${sdk} transforms from Python SDK"
          environment "EXPANSION_JAR", expansionJar
          environment "EXPANSION_PORT", port
          executable 'sh'
          args '-c', ". $envDir/bin/activate && cd $pythonDir && ./scripts/run_integration_test.sh $cmdArgs"
          dependsOn setupTask
          dependsOn config.startJobServer
          if (testJavaHome) {
            environment "JAVA_HOME", testJavaHome
          }
        }
        if (sdk != "Python") {
          mainTask.configure{dependsOn pythonTask}
        }
        cleanupTask.configure{mustRunAfter pythonTask}
        config.cleanupJobServer.configure{mustRunAfter pythonTask}
      }

      // Task for running SQL testcases in Python SDK
      def beamPythonTestPipelineOptions = [
        "pipeline_opts": config.pythonPipelineOptions + sdkLocationOpt,
        "test_opts":  config.pytestOptions,
        "suite": "xlangSqlValidateRunner",
        "collect": "xlang_sql_expansion_service"
      ]
      def cmdArgs = project.project(':sdks:python').mapToArgString(beamPythonTestPipelineOptions)
      def pythonSqlTask = project.tasks.register(config.name+"PythonUsingSql", Exec) {
        group = "Verification"
        description = "Validates runner for cross-language capability of using Java's SqlTransform from Python SDK"
        executable 'sh'
        args '-c', ". $envDir/bin/activate && cd $pythonDir && ./scripts/run_integration_test.sh $cmdArgs"
        dependsOn setupTask
        dependsOn config.startJobServer
        dependsOn ':sdks:java:extensions:sql:expansion-service:shadowJar'
        if (testJavaHome) {
          environment "JAVA_HOME", testJavaHome
        }
      }
      mainTask.configure{dependsOn pythonSqlTask}
      cleanupTask.configure{mustRunAfter pythonSqlTask}
      config.cleanupJobServer.configure{mustRunAfter pythonSqlTask}

      // Task for running Java testcases in Go SDK.
      def pipelineOpts = [
        "--expansion_addr=test:localhost:${javaPort}",
      ]
      def goTask = project.project(":sdks:go:test:").goIoValidatesRunnerTask(project, config.name+"GoUsingJava", config.goScriptOptions, pipelineOpts)
      goTask.configure {
        description = "Validates runner for cross-language capability of using Java transforms from Go SDK"
        dependsOn setupTask
        dependsOn config.startJobServer
      }
      // CrossLanguageValidatesRunnerTask is setup under python sdk but also runs tasks not involving
      // python versions. set 'skipNonPythonTask' property to avoid duplicated run of these tasks.
      if (!(project.hasProperty('skipNonPythonTask') && project.skipNonPythonTask == 'true')) {
        System.err.println 'GoUsingJava tests have been disabled: https://github.com/apache/beam/issues/30517#issuecomment-2341881604.'
        // mainTask.configure { dependsOn goTask }
      }
      cleanupTask.configure { mustRunAfter goTask }
      config.cleanupJobServer.configure { mustRunAfter goTask }
    }

    /** ***********************************************************************************************/

    // Method to create the createTransformServiceTask.
    // The method takes TransformServiceConfiguration as parameter.
    project.ext.createTransformServiceTask = {
      // This task won't work if the python build file doesn't exist.
      if (!project.project(":sdks:python").buildFile.exists()) {
        System.err.println 'Python build file not found. Skipping createTransformServiceTask.'
        return
      }
      def config = it ? it as TransformServiceConfiguration : new TransformServiceConfiguration()

      project.evaluationDependsOn(":sdks:python")
      project.evaluationDependsOn(":sdks:java:extensions:python")
      project.evaluationDependsOn(":sdks:java:transform-service:app")

      def usesDataflowRunner = config.pythonPipelineOptions.contains("--runner=TestDataflowRunner") || config.pythonPipelineOptions.contains("--runner=DataflowRunner")

      // Task for launching transform services
      def envDir = project.project(":sdks:python").envdir
      def pythonDir = project.project(":sdks:python").projectDir
      def externalPort = getRandomPort()
      def launcherJar = project.project(':sdks:java:transform-service:app').shadowJar.archivePath
      def groupId = project.name + randomUUID().toString()
      def transformServiceOpts = [
        "transform_service_launcher_jar": launcherJar,
        "group_id": groupId,
        "external_port": externalPort,
        "beam_version": project.version
      ]
      def serviceArgs = project.project(':sdks:python').mapToArgString(transformServiceOpts)
      def pythonContainerSuffix = project.project(':sdks:python').pythonVersion.replace('.', '')
      def javaContainerSuffix = getSupportedJavaVersion()

      // Transform service delivers transforms that refer to SDK harness containers with following sufixes.
      def transformServiceJavaContainerSuffix = 'java11'
      def transformServicePythonContainerSuffix = '39'

      def setupTask = project.tasks.register(config.name+"Setup", Exec) {
        // Containers for main SDKs when running tests.
        dependsOn ':sdks:java:container:'+javaContainerSuffix+':docker'
        dependsOn ':sdks:python:container:py'+pythonContainerSuffix+':docker'
        // Containers for external SDKs used through the transform service.
        dependsOn ':sdks:java:container:'+transformServiceJavaContainerSuffix+':docker'
        dependsOn ':sdks:python:container:py'+transformServicePythonContainerSuffix+':docker'
        dependsOn ':sdks:java:transform-service:controller-container:docker'
        dependsOn ':sdks:python:expansion-service-container:docker'
        dependsOn ':sdks:java:expansion-service:container:docker'
        dependsOn ":sdks:python:installGcpTest"
        dependsOn project.project(':sdks:java:transform-service:app').shadowJar.getPath()

        if (usesDataflowRunner) {
          dependsOn ":sdks:python:test-suites:dataflow:py${project.ext.pythonVersion.replace('.', '')}:initializeForDataflowJob"
        }

        // setup test env
        executable 'sh'
        args '-c', "$pythonDir/scripts/run_transform_service.sh stop $serviceArgs && $pythonDir/scripts/run_transform_service.sh start $serviceArgs"
      }

      if (config.needsSdkLocation) {
        setupTask.configure {dependsOn ':sdks:python:sdist'}
      }

      def pythonTask = project.tasks.register(config.name+"PythonUsingJava") {
        group = "Verification"
        description = "Runs Python SDK pipeline tests that use transform service"
        dependsOn setupTask
        dependsOn config.startJobServer
        doLast {
          def beamPythonTestPipelineOptions = [
            "pipeline_opts": config.pythonPipelineOptions + (usesDataflowRunner ? [
              "--sdk_location=${project.ext.sdkLocation}"]
            : []),
            "test_opts": config.pytestOptions,
            "suite": config.name,
            "collect": config.collectMarker,
          ]
          def cmdArgs = project.project(':sdks:python').mapToArgString(beamPythonTestPipelineOptions)

          project.exec {
            // Following env variable has to be set to make sure that the tests do not pass trivially.
            environment "TRANSFORM_SERVICE_PORT", externalPort
            executable 'sh'
            args '-c', ". $envDir/bin/activate && cd $pythonDir && ./scripts/run_integration_test.sh $cmdArgs"
          }
        }
      }

      def cleanupTask = project.tasks.register(config.name+'Cleanup', Exec) {
        // teardown test env
        executable 'sh'
        args '-c', "$pythonDir/scripts/run_transform_service.sh stop $serviceArgs"
      }
      setupTask.configure {finalizedBy cleanupTask}
      config.startJobServer.configure {finalizedBy config.cleanupJobServer}

      cleanupTask.configure{mustRunAfter pythonTask}
      config.cleanupJobServer.configure{mustRunAfter pythonTask}
    }

    /** ***********************************************************************************************/

    project.ext.applyPythonNature = {

      // Define common lifecycle tasks and artifact types
      project.apply plugin: "base"

      // For some reason base doesn't define a test task  so we define it below and make
      // check depend on it. This makes the Python project similar to the task layout like
      // Java projects, see https://docs.gradle.org/4.2.1/userguide/img/javaPluginTasks.png
      if (project.tasks.findByName('test') == null) {
        project.task('test') {}
      }
      project.check.dependsOn project.test

      // Due to Beam-4256, we need to limit the length of virtualenv path to make the
      // virtualenv activated properly. So instead of include project name in the path,
      // we use the hash value.
      project.ext.envdir = "${project.rootProject.buildDir}/gradleenv/${project.path.hashCode()}"
      def pythonRootDir = "${project.rootDir}/sdks/python"

      // Python interpreter version for virtualenv setup and test run. This value can be
      // set from commandline with -PpythonVersion, or in build script of certain project.
      // If none of them applied, version set here will be used as default value.
      // TODO(BEAM-12000): Move default value to Py3.9.
      project.ext.pythonVersion = project.hasProperty('pythonVersion') ?
          project.pythonVersion : '3.9'

      def setupVirtualenv = project.tasks.register('setupVirtualenv')  {
        doLast {
          def virtualenvCmd = [
            "python${project.ext.pythonVersion}",
            "-m",
            "venv",
            "--clear",
            "${project.ext.envdir}",
          ]
          project.exec {
            executable 'sh'
            args '-c', virtualenvCmd.join(' ')
          }
          project.exec {
            executable 'sh'
            // TODO: https://github.com/apache/beam/issues/29022
            // pip 23.3 is failing due to Hash mismatch between expected SHA of the packaged and actual SHA.
            // until it is resolved on pip's side, don't use pip's cache.
            // pip 25.1 casues :sdks:python:installGcpTest stuck. Pin to 25.0.1 for now.
            args '-c', ". ${project.ext.envdir}/bin/activate && " +
                "pip install --pre --retries 10 --upgrade pip==25.0.1 --no-cache-dir && " +
                "pip install --pre --retries 10 --upgrade tox --no-cache-dir"
          }
        }
        // Gradle will delete outputs whenever it thinks they are stale. Putting a
        // specific binary here could make gradle delete it while pip will believe
        // the package is fully installed.
        outputs.dirs(project.ext.envdir)
        outputs.upToDateWhen { false }
      }

      project.ext.pythonSdkDeps = project.files(
          project.fileTree(
          dir: "${project.rootDir}",
          include: ['model/**', 'sdks/python/**'],
          // Exclude temporary directories and files that are generated
          // during build and test.
          exclude: [
            '**/build/**',
            '**/dist/**',
            '**/target/**',
            '**/.gogradle/**',
            '**/*.pyc',
            'sdks/python/*.egg*/**',
            'sdks/python/test-suites/**',
            'sdks/python/__pycache__',
            '**/reports/test/index.html',
          ])
          )
      def copiedSrcRoot = "${project.buildDir}/srcs"

      // Create new configuration distTarBall which represents Python source
      // distribution tarball generated by :sdks:python:sdist.
      project.configurations { distTarBall }

      def installGcpTest = project.tasks.register('installGcpTest')  {
        dependsOn setupVirtualenv
        dependsOn ':sdks:python:sdist'
        doLast {
          def distTarBall = "${pythonRootDir}/build/apache-beam.tar.gz"
          def packages = "gcp,test,aws,azure,dataframe"
          def extra = project.findProperty('beamPythonExtra')
          if (extra) {
            packages += ",${extra}"
          }

          project.exec {
            executable 'sh'
            args '-c', ". ${project.ext.envdir}/bin/activate && pip install --pre --retries 10 ${distTarBall}[${packages}]"
          }
        }
      }

      def cleanPython = project.tasks.register('cleanPython') {
        doLast {
          def activate = "${project.ext.envdir}/bin/activate"
          project.delete project.buildDir     // Gradle build directory
          project.delete project.ext.envdir   // virtualenv directory
          project.delete "$project.projectDir/target"   // tox work directory
        }
      }
      project.clean.dependsOn cleanPython
      // Force this subproject's clean to run before the main :clean, to avoid
      // racing on deletes.
      project.rootProject.clean.dependsOn project.clean

      // Return a joined String from a Map that contains all commandline args of
      // IT test.
      project.ext.mapToArgString = { argMap ->
        def argList = []
        argMap.each { k, v ->
          if (v in List) {
            v = "\"${v.join(' ')}\""
          } else if (v in String && v.contains(' ')) {
            // We should use double quote around the arg value if it contains series
            // of flags joined with space. Otherwise, commandline parsing of the
            // shell script will be broken.
            // Remove all double quotes except those followed with a backslash.
            v = "\"${v.replaceAll('(?<!\\\\)"', '')}\""
          }
          argList.add("--$k $v")
        }
        return argList.join(' ')
      }
      project.ext.toxTask = { name, tox_env, posargs='' ->
        project.tasks.register(name) {
          dependsOn setupVirtualenv
          dependsOn ':sdks:python:sdist'

          def testJavaVersion = project.findProperty('testJavaVersion')
          String testJavaHome = null
          if (testJavaVersion) {
            testJavaHome = project.findProperty("java${testJavaVersion}Home")
          }

          if (project.hasProperty('useWheelDistribution')) {
            def pythonVersionNumber  = project.ext.pythonVersion.replace('.', '')
            dependsOn ":sdks:python:bdistPy${pythonVersionNumber}linux"
            doLast {
              project.copy { from project.pythonSdkDeps; into copiedSrcRoot }
              def copiedPyRoot = "${copiedSrcRoot}/sdks/python"
              def collection = project.fileTree(project.project(':sdks:python').buildDir){
                include "**/apache_beam-*cp${pythonVersionNumber}*manylinux*.whl"
              }
              String packageFilename = collection.singleFile.toString()
              project.exec {
                if (testJavaHome) {
                  environment "JAVA_HOME", testJavaHome
                }
                executable 'sh'
                args '-c', ". ${project.ext.envdir}/bin/activate && cd ${copiedPyRoot} && scripts/run_tox.sh $tox_env ${packageFilename} '$posargs' "
              }
            }
          } else {
            // tox task will run in editable mode, which is configured in the tox.ini file.
            doLast {
              project.copy { from project.pythonSdkDeps; into copiedSrcRoot }
              def copiedPyRoot = "${copiedSrcRoot}/sdks/python"
              project.exec {
                if (testJavaHome) {
                  environment "JAVA_HOME", testJavaHome
                }
                executable 'sh'
                args '-c', ". ${project.ext.envdir}/bin/activate && cd ${copiedPyRoot} && scripts/run_tox.sh $tox_env '$posargs'"
              }
            }
          }
          inputs.files project.pythonSdkDeps
          outputs.files project.fileTree(dir: "${pythonRootDir}/target/.tox/${tox_env}/log/")
        }
      }
      // Run single or a set of integration tests with provided test options and pipeline options.
      project.ext.enablePythonPerformanceTest = {

        // Use the implicit it parameter of the closure to handle zero argument or one argument map calls.
        // See: http://groovy-lang.org/closures.html#implicit-it
        def config = it ? it as PythonPerformanceTestConfiguration : new PythonPerformanceTestConfiguration()

        project.tasks.register('integrationTest') {
          dependsOn installGcpTest
          dependsOn ':sdks:python:sdist'

          doLast {
            def argMap = [:]

            // Build test options that configures test environment and framework
            def testOptions = []
            if (config.tests)
              testOptions += "$config.tests"
            if (config.attribute)
              testOptions += "-m=$config.attribute"
            testOptions.addAll(config.extraTestOptions)
            argMap["test_opts"] = testOptions

            // Build pipeline options that configures pipeline job
            if (config.pipelineOptions)
              argMap["pipeline_opts"] = config.pipelineOptions
            if (config.kmsKeyName)
              argMap["kms_key_name"] = config.kmsKeyName
            argMap["suite"] = "integrationTest-perf"

            def cmdArgs = project.mapToArgString(argMap)
            def runScriptsDir = "${pythonRootDir}/scripts"
            project.exec {
              executable 'sh'
              args '-c', ". ${project.ext.envdir}/bin/activate && ${runScriptsDir}/run_integration_test.sh ${cmdArgs}"
            }
          }
        }
      }

      def addPortableWordCountTask = { boolean isStreaming, String runner ->
        def taskName = 'portableWordCount' + runner + (isStreaming ? 'Streaming' : 'Batch')
        def flinkJobServerProject = ":runners:flink:${project.ext.latestFlinkVersion}:job-server"
        project.tasks.register(taskName) {
          dependsOn = [installGcpTest]
          mustRunAfter = [
            ":runners:flink:${project.ext.latestFlinkVersion}:job-server:shadowJar",
            ':runners:spark:3:job-server:shadowJar',
            ':sdks:python:container:py39:docker',
            ':sdks:python:container:py310:docker',
            ':sdks:python:container:py311:docker',
            ':sdks:python:container:py312:docker',
          ]
          doLast {
            // TODO: Figure out GCS credentials and use real GCS input and output.
            def outputDir = File.createTempDir(taskName, '')
            def options = [
              "--input=/etc/profile",
              "--output=${outputDir}/out.txt",
              "--runner=${runner}",
              "--parallelism=2",
              "--sdk_worker_parallelism=1",
              "--flink_job_server_jar=${project.project(flinkJobServerProject).shadowJar.archivePath}",
              "--spark_job_server_jar=${project.project(':runners:spark:3:job-server').shadowJar.archivePath}",
            ]
            if (isStreaming)
              options += [
                "--streaming"
              ]
            else
              // workaround for local file output in docker container
              options += [
                "--environment_cache_millis=60000"
              ]
            if (project.hasProperty("jobEndpoint"))
              options += [
                "--job_endpoint=${project.property('jobEndpoint')}"
              ]
            if (project.hasProperty("environmentType")) {
              options += [
                "--environment_type=${project.property('environmentType')}"
              ]
            }
            if (project.hasProperty("environmentConfig")) {
              options += [
                "--environment_config=${project.property('environmentConfig')}"
              ]
            }
            project.exec {
              executable 'sh'
              args '-c', ". ${project.ext.envdir}/bin/activate && python -m apache_beam.examples.wordcount ${options.join(' ')}"
              // TODO: Check that the output file is generated and runs.
            }
          }
        }
      }
      project.ext.addPortableWordCountTasks = {
        ->
        addPortableWordCountTask(false, "FlinkRunner")
        addPortableWordCountTask(true, "FlinkRunner")
        addPortableWordCountTask(false, "SparkRunner")
      }

      project.ext.getVersionSuffix = { String version ->
        return version.replace('.', '')
      }

      project.ext.getVersionsAsList = { String propertyName ->
        return project.getProperty(propertyName).split(',')
      }
    }
  }

  private void setAutomaticModuleNameHeader(JavaNatureConfiguration configuration, Project project) {
    if (configuration.publish && !configuration.automaticModuleName) {
      throw new GradleException("Expected automaticModuleName to be set for the module that is published to maven repository.")
    } else if (configuration.automaticModuleName) {
      project.jar.manifest {
        attributes 'Automatic-Module-Name': configuration.automaticModuleName
      }
    }
  }
}
