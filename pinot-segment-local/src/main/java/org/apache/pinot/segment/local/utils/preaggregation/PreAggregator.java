/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.utils.preaggregation;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregatorFactory;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.PreAggregationConfig;


public class PreAggregator {
  private final Map<String, String> _aggregatorColumnNameToMetricColumnName;
  private final Map<String, ValueAggregator> _aggregators;

  private static final Set<AggregationFunctionType> ALLOWED_TYPES = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList(AggregationFunctionType.SUM, AggregationFunctionType.MIN, AggregationFunctionType.MAX)));

  public static PreAggregator fromRealtimeSegmentConfig(RealtimeSegmentConfig segmentConfig) {
    if (segmentConfig == null || !segmentConfig.aggregateMetrics() || segmentConfig.getPreAggregationConfigs() == null
        || segmentConfig.getPreAggregationConfigs().size() == 0) {
      return new PreAggregator();
    }

    Map<String, String> destColumnToSrcColumn = new HashMap<>();
    Map<String, ValueAggregator> aggregators = new HashMap<>();

    for (PreAggregationConfig config : segmentConfig.getPreAggregationConfigs()) {
      AggregationFunctionType functionType =
          AggregationFunctionType.getAggregationFunctionType(config.getTransformFunction());

      Preconditions.checkState(ALLOWED_TYPES.contains(functionType), "AggregationFunctionType %s not supported",
          functionType);

      destColumnToSrcColumn.put(config.getAggregatedColumnName(), config.getColumnName());
      aggregators.put(config.getAggregatedColumnName(), ValueAggregatorFactory.getValueAggregator(functionType));
    }

    return new PreAggregator(destColumnToSrcColumn, aggregators);
  }

  private PreAggregator() {
    _aggregatorColumnNameToMetricColumnName = null;
    _aggregators = null;
  }

  private PreAggregator(Map<String, String> aggregatorColumnNameToMetricColumnName, Map<String, ValueAggregator> aggregators) {
    _aggregatorColumnNameToMetricColumnName = aggregatorColumnNameToMetricColumnName;
    _aggregators = aggregators;
  }

  public String getMetricName(String aggregatedColumnName) {
    if (_aggregatorColumnNameToMetricColumnName == null) {
      return aggregatedColumnName;
    }
    return _aggregatorColumnNameToMetricColumnName.getOrDefault(aggregatedColumnName, aggregatedColumnName);
  }

  @Nullable
  public ValueAggregator getAggregator(String aggregatedColumnName) {
    if (_aggregators == null) {
      return null;
    }
    return _aggregators.getOrDefault(aggregatedColumnName, null);
  }
}
