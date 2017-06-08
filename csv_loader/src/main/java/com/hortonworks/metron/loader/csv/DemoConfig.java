/**
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
package com.hortonworks.metron.loader.csv;

import org.apache.metron.common.csv.CSVConverter;

import java.util.List;
import java.util.Map;

public class DemoConfig {

  public static class DataSource {
    public String outputTopic;
    public String inputFile;
    public String filter;
    public CSVConverter converter;

    public String getOutputTopic() {
      return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
      this.outputTopic = outputTopic;
    }

    public String getInputFile() {
      return inputFile;
    }

    public void setInputFile(String inputFile) {
      this.inputFile = inputFile;
    }

    public CSVConverter getConverter() {
      return converter;
    }

    public void setConfig(Map<String, Object> config) {
      converter = new CSVConverter();
      converter.initialize(config);
    }

    public String getFilter() {
      return filter;
    }

    public void setFilter(String filter) {
      this.filter = filter;
    }
  }

  public int stepTimeMs;
  public Long timeOffset;
  public List<DataSource> sources;

  public int getStepTimeMs() {
    return stepTimeMs;
  }

  public void setStepTimeMs(int stepTimeMs) {
    this.stepTimeMs = stepTimeMs;
  }

  public Long getTimeOffset() {
    return timeOffset;
  }

  public void setTimeOffset(Long timeOffset) {
    this.timeOffset = timeOffset;
  }

  public List<DataSource> getSources() {
    return sources;
  }

  public void setSources(List<DataSource> sources) {
    this.sources = sources;
  }
}
