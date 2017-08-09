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


import org.apache.metron.guava.base.Splitter;
import org.apache.metron.guava.collect.Iterables;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class DegreeAnalyzer {

  public static void processRecords(File f, Function<Map.Entry<String, String>, Void> op) throws IOException {
    BufferedReader reader = Files.newBufferedReader(f.toPath(), Charset.defaultCharset());
    for(String line = null;(line = reader.readLine()) != null;) {
      Iterable<String> tokens = Splitter.on(",").split(line);
      String left = Iterables.getFirst(tokens, "").trim();
      String right = Iterables.getLast(tokens, "").trim();
      if(left.length() > 0 && right.length() > 0) {
        Map.Entry<String, String> lr = new AbstractMap.SimpleEntry<String, String>(left.trim(), right.trim());
        op.apply(lr);
      }
    }
  }

  public static void main(String... argv) throws IOException {
    final ArrayList<String> hosts = new ArrayList<>();
    final Map<String, Integer> hostToIndex = new HashMap<>();
    File inputFile = new File(argv[0]);
    List<String> importantHosts = new ArrayList<>();
    for(int i = 1;i < argv.length;++i) {
      importantHosts.add(argv[i].trim());
    }

    Progress progress = new Progress(System.err);
    AtomicInteger numRecs = new AtomicInteger(0);
    processRecords(inputFile
            , entry -> {
              if(!hostToIndex.containsKey(entry.getKey())) {
                int idx = hosts.size();
                hosts.add(entry.getKey());
                hostToIndex.put(entry.getKey(), idx);
              }
              if(!hostToIndex.containsKey(entry.getValue())) {
                int idx = hosts.size();
                hosts.add(entry.getValue());
                hostToIndex.put(entry.getValue(), idx);
              }
              numRecs.incrementAndGet();
              progress.update();
              return null;
            }
    );
    progress.reset();
    int tenPercent = numRecs.get()/10;
    System.err.println("Preprocessed " + numRecs.get() + " records.");
    final boolean[][] adjacencyMatrix = new boolean[hosts.size()][hosts.size()];
    for(int i = 0;i < hosts.size();++i) {
      for(int j = 0;j < hosts.size();++j) {
        adjacencyMatrix[i][j] = i == j;
      }
    }
    final AtomicInteger proc = new AtomicInteger(0);
    processRecords( inputFile
            , entry -> {
              int row = hostToIndex.get(entry.getKey());
              int col = hostToIndex.get(entry.getValue());
              adjacencyMatrix[row][col] = true;
              int numProcessed = proc.incrementAndGet();
              progress.update();
              if(tenPercent > 0 && numProcessed % tenPercent == 0) {
                System.err.println(" -- " + (numProcessed / tenPercent) + " %");
              }
              return null;
            }
    );
    System.err.println("\nSquaring adjacency matrix (" + adjacencyMatrix.length + "x" + adjacencyMatrix.length + ") to make transitive links...");
    List<Integer> rowIds = new ArrayList<>();
    for(String importantHost : importantHosts) {
      Integer idx = hostToIndex.get(importantHost);
      if(idx != null) {
        rowIds.add(idx);
      }
    }
    boolean[][] transitiveMatrix = squareMatrix(adjacencyMatrix, rowIds);
    Set<String> ret = new HashSet<>();
    for(Integer idx : rowIds) {
      boolean[] connectedHosts = transitiveMatrix[idx];
      for(int i = 0;i < connectedHosts.length;++i) {
        if(connectedHosts[i]) {
          ret.add(hosts.get(i));
        }
      }
    }
    for(String s : ret) {
      System.out.println(s);
    }
  }

  private static boolean innerProduct(boolean[][] adjacencyMatrix, int rowId, int colId) {
    boolean ret = false;
    for(int i = 0;i < adjacencyMatrix.length;++i) {
      boolean lhs = adjacencyMatrix[rowId][i];
      boolean rhs = adjacencyMatrix[i][colId];
      ret = ret | (rhs && lhs);
    }
    return ret;
  }

  private static boolean[][] squareMatrix(boolean[][] adjacencyMatrix, List<Integer> rowIds) {
    boolean[][] ret = new boolean[adjacencyMatrix.length][adjacencyMatrix.length];
    for(Integer i : rowIds) {
      for(int j = 0;j < adjacencyMatrix.length;++j) {
        ret[i][j] = innerProduct(adjacencyMatrix, i, j);
      }
    }
    return ret;
  }

}
