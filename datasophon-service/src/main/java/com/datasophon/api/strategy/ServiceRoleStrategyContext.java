/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datasophon.api.strategy;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceRoleStrategyContext {
    private static final ServiceRoleStrategy commonStrategy=new CommonHandlerStrategy();

    private static final String commonServiceName="CommonService";

    private static final Map<String, ServiceRoleStrategy> strategyMap = new ConcurrentHashMap<>();
    
    private static final Map<String, String> serviceNameMap = new ConcurrentHashMap<>();
    
    static {
        strategyMap.put("NameNode", new NameNodeHandlerStrategy());
        strategyMap.put("ResourceManager", new RMHandlerStrategy());
        strategyMap.put("HiveMetaStore", new HiveMetaStroreHandlerStrategy());
        strategyMap.put("HiveServer2", new HiveServer2HandlerStrategy());
        strategyMap.put("Grafana", new GrafanaHandlerStrategy());
        strategyMap.put("ZkServer", new ZkServerHandlerStrategy());
        strategyMap.put("HistoryServer", new HistoryServerHandlerStrategy());
        strategyMap.put("TimelineServer", new TSHandlerStrategy());
        strategyMap.put("TrinoCoordinator", new TrinoHandlerStrategy());
        strategyMap.put("JournalNode", new JournalNodeHandlerStrategy());
        strategyMap.put("ZKFC", new ZKFCHandlerStrategy());
        strategyMap.put("HttpFs", new HttpFsHandlerStrategy());
        strategyMap.put("SRFE", new FEHandlerStartegy());
        strategyMap.put("DorisFE", new FEHandlerStartegy());
        strategyMap.put("DorisFEObserver", new FEObserverHandlerStartegy());
        strategyMap.put("SRBE", new BEHandlerStartegy());
        strategyMap.put("DorisBE", new BEHandlerStartegy());
        strategyMap.put("Krb5Kdc", new Krb5KdcHandlerStrategy());
        strategyMap.put("KAdmin", new KAdminHandlerStrategy());
        strategyMap.put("RangerAdmin", new RangerAdminHandlerStrategy());
        strategyMap.put("ElasticSearch", new ElasticSearchHandlerStrategy());
        strategyMap.put("Prometheus", new PrometheusHandlerStrategy());
        strategyMap.put("AlertManager", new AlertManagerHandlerStrategy());
        
        strategyMap.put("RANGER", new RangerAdminHandlerStrategy());
        strategyMap.put("ZOOKEEPER", new ZkServerHandlerStrategy());
        strategyMap.put("YARN", new RMHandlerStrategy());
        strategyMap.put("HDFS", new NameNodeHandlerStrategy());
        strategyMap.put("HIVE", new HiveServer2HandlerStrategy());
        strategyMap.put("KAFKA", new KafkaHandlerStrategy());
        strategyMap.put("HBASE", new HBaseHandlerStrategy());
        strategyMap.put("FLINK", new FlinkHandlerStrategy());
        strategyMap.put("KYUUBI", new KyuubiServerHandlerStrategy());
        
        // serviceNameMap
        serviceNameMap.put("NameNode", "HDFS");
        serviceNameMap.put("ResourceManager", "YARN");
        serviceNameMap.put("HiveMetaStore", "HIVE");
        serviceNameMap.put("HiveServer2", "HIVE");
        serviceNameMap.put("Grafana", "GRAFANA");
        serviceNameMap.put("ZkServer", "ZOOKEEPER");
        serviceNameMap.put("HistoryServer", "YARN");
        serviceNameMap.put("TimelineServer", "YARN");
        serviceNameMap.put("TrinoCoordinator", "TRINO");
        serviceNameMap.put("JournalNode", "HDFS");
        serviceNameMap.put("ZKFC", "HDFS");
        serviceNameMap.put("HttpFs", "HDFS");
        serviceNameMap.put("SRFE", "STARROCKS");
        serviceNameMap.put("DorisFE", "DORIS");
        serviceNameMap.put("DorisFEObserver", "DORIS");
        serviceNameMap.put("SRBE", "STARROCKS");
        serviceNameMap.put("DorisBE", "DORIS");
        serviceNameMap.put("Krb5Kdc", "KERBEROS");
        serviceNameMap.put("KAdmin", "KERBEROS");
        serviceNameMap.put("RangerAdmin", "RANGER");
        serviceNameMap.put("ElasticSearch", "ELASTICSEARCH");
        serviceNameMap.put("Prometheus", "PROMETHEUS");
        serviceNameMap.put("AlertManager", "ALERTMANAGER");
        
        serviceNameMap.put("FLINK", "FLINK");
        serviceNameMap.put("RANGER", "RANGER");
        serviceNameMap.put("YARN", "YARN");
        serviceNameMap.put("HDFS", "HDFS");
        serviceNameMap.put("HIVE", "HIVE");
        serviceNameMap.put("KAFKA", "KAFKA");
        serviceNameMap.put("HBASE", "HBASE");
        serviceNameMap.put("KYUUBI", "KYUUBI");
        
    }
    
    public static ServiceRoleStrategy getServiceRoleHandler(String type) {
        if (StringUtils.isBlank(type)) {
            return null;
        }
        ServiceRoleStrategy strategy=strategyMap.get(type);
        return (strategy == null) ? commonStrategy : strategy;
    }
    
    public static String getServiceName(String type) {
        if (StringUtils.isBlank(type)) {
            return null;
        }
        String name=serviceNameMap.get(type);
        return (name == null) ? commonServiceName : name;
    }
}
