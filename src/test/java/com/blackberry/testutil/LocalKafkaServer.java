/**
 * Copyright 2014 BlackBerry, Inc.
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

package com.blackberry.testutil;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import org.apache.commons.io.FileUtils;

public class LocalKafkaServer {
  private String nodeId = "0";
  private String port = "9876";
  private String logDir = FileUtils.getTempDirectoryPath() + "/kafka.log";
  private String zkConnect = "localhost:21818";
  private KafkaServerStartable server;

  public LocalKafkaServer() throws IOException {

    while (new File(logDir).exists()) {
      FileUtils.deleteDirectory(new File(logDir));
    }

    Properties props = new Properties();
    props.put("broker.id", nodeId);
    props.put("port", port);
    props.put("log.dir", logDir);
    props.put("zookeeper.connect", zkConnect);
    props.put("host.name", "127.0.0.1");
    KafkaConfig conf = new KafkaConfig(props);

    server = new KafkaServerStartable(conf);
    server.startup();
  }

  public void shutdown() throws IOException {
    server.shutdown();
    server.awaitShutdown();
    FileUtils.deleteDirectory(new File(logDir));
  }

  public void createTopic(String topic) {
    TopicCommand.main(new String[] { "--create", "--zookeeper",
        "localhost:21818", "--replication-factor", "1", "--partition", "1",
        "--topic", topic });
  }
}
