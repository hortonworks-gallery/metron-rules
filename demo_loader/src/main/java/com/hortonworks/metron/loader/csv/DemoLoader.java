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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.metron.common.dsl.Context;
import org.apache.metron.common.dsl.MapVariableResolver;
import org.apache.metron.common.dsl.StellarFunctions;
import org.apache.metron.common.dsl.VariableResolver;
import org.apache.metron.common.stellar.StellarPredicateProcessor;
import org.apache.metron.common.utils.JSONUtils;
import org.apache.metron.common.utils.KafkaUtils;
import org.apache.metron.common.utils.cli.OptionHandler;
import org.apache.metron.dataloads.nonbulk.flatfile.importer.LocalImporter;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class DemoLoader {
  public enum LoadOptions {
    HELP("h", new OptionHandler<LoadOptions>() {

      @Nullable
      @Override
      public Option apply(@Nullable String s) {
        return new Option(s, "help", false, "Generate Help screen");
      }
    }),
    CONFIG("c", new OptionHandler<LoadOptions>() {
      @Nullable
      @Override
      public Option apply(@Nullable String s) {
        Option o = new Option(s, "config", true, "The demo config.");
        o.setArgName("CONFIG_FILE");
        o.setRequired(true);
        return o;
      }

      @Override
      public Optional<Object> getValue(LoadOptions option, CommandLine cli) {
        return Optional.ofNullable(option.get(cli).trim());
      }
    }),
    ZK_QUORUM("z", new OptionHandler<LoadOptions>() {
      @Nullable
      @Override
      public Option apply(@Nullable String s) {
        Option o = new Option(s, "zk_quorum", true, "The zookeeper quorum.");
        o.setArgName("QUORUM");
        o.setRequired(false);
        return o;
      }

      @Override
      public Optional<Object> getValue(LoadOptions option, CommandLine cli) {
        return Optional.ofNullable(option.get(cli).trim());
      }
    }),
    HOSTNAME_FILTER("hf", new OptionHandler<LoadOptions>() {
      @Nullable
      @Override
      public Option apply(@Nullable String s) {
        Option o = new Option(s, "host_filter", true, "The Hostname filter.");
        o.setArgName("FILTER");
        o.setRequired(false);
        return o;
      }

      @Override
      public Optional<Object> getValue(LoadOptions option, CommandLine cli) {
        return Optional.ofNullable(option.get(cli).trim());
      }
    }),
    KAFKA_PRODUCER_CONFIGS("kp", new OptionHandler<LoadOptions>() {
      @Nullable
      @Override
      public Option apply(@Nullable String s) {
        Option o = new Option(s, "producer_config", true, "The kafka producer configs.");
        o.setArgName("CONFIG_FILE");
        o.setRequired(false);
        return o;
      }

      @Override
      public Optional<Object> getValue(LoadOptions option, CommandLine cli) {
        return Optional.ofNullable(option.get(cli).trim());
      }
    }),
    TIME_START("s", new OptionHandler<LoadOptions>() {
      @Nullable
      @Override
      public Option apply(@Nullable String s) {
        Option o = new Option(s, "start_time", true, "Time to start loading the data.");
        o.setArgName("TS");
        o.setRequired(false);
        return o;
      }

      @Override
      public Optional<Object> getValue(LoadOptions option, CommandLine cli) {
        return Optional.ofNullable(option.get(cli).trim());
      }
    }),
    TIME_END("e", new OptionHandler<LoadOptions>() {
      @Nullable
      @Override
      public Option apply(@Nullable String s) {
        Option o = new Option(s, "end_time", true, "Time to end loading the data.");
        o.setArgName("TS");
        o.setRequired(false);
        return o;
      }

      @Override
      public Optional<Object> getValue(LoadOptions option, CommandLine cli) {
        return Optional.ofNullable(option.get(cli).trim());
      }
    });
    Option option;
    String shortCode;
    OptionHandler<LoadOptions> handler;
    LoadOptions(String shortCode, OptionHandler<LoadOptions> optionHandler) {
      this.shortCode = shortCode;
      this.handler = optionHandler;
      this.option = optionHandler.apply(shortCode);
    }

    public boolean has(CommandLine cli) {
      return cli.hasOption(shortCode);
    }

    public String get(CommandLine cli) {
      return cli.getOptionValue(shortCode);
    }

    public static Options getOptions() {
      Options ret = new Options();
      for(LoadOptions o : LoadOptions.values()) {
        ret.addOption(o.option);
      }
      return ret;
    }

    public static void printHelp() {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "DemoLoader", getOptions());
    }

    public static CommandLine parse(CommandLineParser parser, String[] args) {
      try {
        CommandLine cli = parser.parse(getOptions(), args);
        if(HELP.has(cli)) {
          printHelp();
          System.exit(0);
        }
        return cli;
      } catch (ParseException e) {
        System.err.println("Unable to parse args: "
                + String.join(" ", args)
        );
        e.printStackTrace(System.err);
        printHelp();
        System.exit(-1);
        return null;
      }
    }
  }

  private static KafkaProducer<String, String> createProducer(List<String> brokers, Map<String, Object> config) {
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("bootstrap.servers", brokers.get(0));
    producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerConfig.put("request.required.acks", 1);
    producerConfig.putAll(config);
    return new KafkaProducer<String, String>(producerConfig);
  }

  public static void main(String... argv) throws Exception {
    CommandLine cli = LoadOptions.parse(new PosixParser(), argv);
    DemoConfig config = JSONUtils.INSTANCE.load(new File(LoadOptions.CONFIG.get(cli)), DemoConfig.class);
    int stepTimeMs = config.getStepTimeMs();
    Long timeOffset = Optional.ofNullable(config.getTimeOffset()).orElse(System.currentTimeMillis());

    int start = 1;
    if(LoadOptions.TIME_START.has(cli)) {
      start = Integer.parseInt(LoadOptions.TIME_START.get(cli));
    }

    int end = -1;
    if(LoadOptions.TIME_END.has(cli)) {
      end = Integer.parseInt(LoadOptions.TIME_END.get(cli));
    }

    KafkaProducer<String, String> producer = null;
    if(LoadOptions.ZK_QUORUM.has(cli)) {
      Map<String, Object> kafkaConfigs = new HashMap<>();
      if (LoadOptions.KAFKA_PRODUCER_CONFIGS.has(cli)) {
        kafkaConfigs = JSONUtils.INSTANCE.load(new File(LoadOptions.KAFKA_PRODUCER_CONFIGS.get(cli)), new TypeReference<Map<String, Object>>() {
        });
      }
      List<String> brokers = KafkaUtils.INSTANCE.getBrokersFromZookeeper(LoadOptions.ZK_QUORUM.get(cli));
      producer = createProducer(brokers, kafkaConfigs);
    }
    Set<String> hostnameFilter = null;
    if(LoadOptions.HOSTNAME_FILTER.has(cli)) {
      try(BufferedReader br = Files.newBufferedReader(new File(LoadOptions.HOSTNAME_FILTER.get(cli)).toPath(), Charset.defaultCharset())) {

        hostnameFilter = new HashSet<>();
        for(String line = null;(line = br.readLine()) != null;) {
          if(line.trim().length() > 0) {
            hostnameFilter.add(line.trim());
          }
        }
      }
    }
    System.err.println("Loading the following sources:");
    for(DemoConfig.DataSource ds : config.getSources()) {
      System.err.println("\t" + toFileName(ds.getInputFile()) + " => " + ds.getOutputTopic());
    }
    System.err.println("Time step set to " + stepTimeMs + "ms.");
    System.err.println("Time offset set to " + timeOffset + " ms since Unix epoch.");
    System.err.println("Going from offset " + start + " to " + end);
    step(start, end, config, producer, timeOffset, stepTimeMs, hostnameFilter);
  }

  public static String toFileName(String path) throws IOException {
    File f = new File(path);
    String fileName = f.getName();
    if(Files.isSymbolicLink(f.toPath())) {
      Path p = Files.readSymbolicLink(f.toPath());
      fileName = p.getFileName().toFile().getName();
    }
    return fileName;
  }

  public static class SourceState {
    DemoConfig.DataSource source;
    Map<String, Object> buffer;
    BufferedReader reader;
    public boolean allDone = false;
    public SourceState(DemoConfig.DataSource source, BufferedReader reader) {
      this.source = source;
      this.reader = reader;
    }

    public DemoConfig.DataSource getSource() {
      return source;
    }

    public BufferedReader getReader() {
      return reader;
    }

    private void updateBuffer() throws IOException {
      String line = reader.readLine();
      if(line != null) {
        //System.out.println("Read " + line + " from " + getSource().getInputFile());
        Map<String, String> v = source.getConverter().toMap(line);
        buffer = new HashMap<>();
        buffer.putAll(v);
      }
      else {
        //System.out.println("Finished reading " + getSource().getInputFile());
        allDone = true;
        buffer = null;
        try {
          reader.close();
        }
        catch(Exception ex){
          throw new IllegalStateException("Unable to close reader.", ex);
        }
      }
    }

    private Integer getTimestamp() {
      if(buffer == null) {
        return null;
      }
      Object tsObj = buffer == null?null:buffer.get("timestamp");
      if(tsObj != null) {
        return Integer.parseInt("" + tsObj);
      }
      else {
        throw new IllegalStateException("You must have at least a timestamp field.");
      }
    }

    public List<Map<String, Object>> read(int step) throws IOException {
      List<Map<String, Object>> ret= new ArrayList<>();
      if(allDone) {
        return ret;
      }
      if(buffer == null && !allDone) {
        updateBuffer();
      }
      for( Integer ts = getTimestamp()
         ; !allDone && ts != null && ts <= step
         ; updateBuffer(),ts=getTimestamp()
         )  {
        if (ts == step) {
          //System.out.println(ts + " == " + step + ": Added " + buffer + " to " + getSource().getInputFile());
          ret.add(buffer);
        }
        else {
          //System.out.println("Missed " + ts + " != " + step + " in " + getSource().getInputFile());
        }
      }
      return ret;
    }
  }


  public static void step( int start
                         , int end
                         , DemoConfig config
                         , KafkaProducer<String, String> producer
                         , Long timeOffset
                         , Integer stepTimeMs
                         , Set<String> hostnameFilter
                         ) throws IOException, InterruptedException {
    List<SourceState> states = new ArrayList<>();
    for(DemoConfig.DataSource ds : config.getSources()) {
      BufferedReader reader = null;
      if(ds.getInputFile().endsWith(".gz")) {
        reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(ds.getInputFile())), Charset.defaultCharset()));
      }
      else {
        reader = Files.newBufferedReader(new File(ds.getInputFile()).toPath(), Charset.defaultCharset());
      }
      states.add(new SourceState(ds, reader));
    }
    LocalImporter.Progress progress = new LocalImporter.Progress();
    try {
      for (int index = start; index <= end; ++index) {
        progress.update();
        boolean allDone = true;
        long sTime = System.currentTimeMillis();
        int numMessagesWritten = 0;
        for (SourceState s : states) {
          String fileName = toFileName(s.getSource().getInputFile());
          List<Map<String, Object>> messages = s.read(index);
          for (Map<String, Object> message : messages) {
            int timestamp = Integer.parseInt("" + message.get("timestamp"));
            message.put("time_offset", timestamp);
            message.put("source_file", fileName);
            message.put("timestamp", 1000L*timestamp + timeOffset);
            if(matchesFilter(s.getSource(), hostnameFilter, message)) {
              String jsonMap = JSONUtils.INSTANCE.toJSON(message, false);
              if(producer != null) {
                numMessagesWritten++;
                producer.send(new ProducerRecord<String, String>(s.getSource().getOutputTopic(), jsonMap));
              }
              else {
                System.out.println(jsonMap);
              }
            }
          }
          allDone &= s.allDone;
        }
        if(allDone) {
          break;
        }
        else if(numMessagesWritten > 0 && producer != null){
          long eTime = System.currentTimeMillis();
          long durationMs = eTime - sTime;
          if(durationMs < stepTimeMs) {
            long msSleeping = stepTimeMs - durationMs;
            Thread.sleep(msSleeping);
          }
        }
      }
    }
    finally {
      if(producer != null) {
        producer.close();
      }
    }
  }

  static boolean matchesFilter(DemoConfig.DataSource ds, Set<String> hostnameFilter, Map<String, Object> message) {
    StellarPredicateProcessor processor = new StellarPredicateProcessor();
    VariableResolver vr = null;
    if(hostnameFilter != null) {
      Map<String, Object> hf = new HashMap<>();
      hf.put("hostname_filter", hostnameFilter);
      vr = new MapVariableResolver(message, hf);
    }
    else {
      vr = new MapVariableResolver(message);
    }
    return processor.parse(ds.getFilter(), vr, StellarFunctions.FUNCTION_RESOLVER(), Context.EMPTY_CONTEXT());
  }
}
