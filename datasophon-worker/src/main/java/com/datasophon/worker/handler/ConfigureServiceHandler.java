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

package com.datasophon.worker.handler;

import com.datasophon.common.Constants;
import com.datasophon.common.model.Generators;
import com.datasophon.common.model.RunAs;
import com.datasophon.common.model.ServiceConfig;
import com.datasophon.common.utils.ExecResult;
import com.datasophon.common.utils.PlaceholderUtils;
import com.datasophon.common.utils.ShellUtils;
import com.datasophon.worker.utils.FreemakerUtils;
import com.datasophon.worker.utils.TaskConstants;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import lombok.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.IdUtil;

@Data
public class ConfigureServiceHandler {
    
    private static final String RANGER_ADMIN = "RangerAdmin";
    
    private static final String SH = "sh";
    
    private String serviceName;
    
    private String serviceRoleName;
    
    private Logger logger;
    
    public ConfigureServiceHandler(String serviceName, String serviceRoleName) {
        this.serviceName = serviceName;
        this.serviceRoleName = serviceRoleName;
        String loggerName = String.format("%s-%s-%s", TaskConstants.TASK_LOG_LOGGER_NAME, serviceName, serviceRoleName);
        logger = LoggerFactory.getLogger(loggerName);
    }
    
    public ExecResult configure(Map<Generators, List<ServiceConfig>> configFileMap,
                                String decompressPackageName,
                                Integer clusterId,
                                Integer myid,
                                String serviceRoleName,
                                RunAs runAs) {
        ExecResult execResult = new ExecResult();
        try {
            
            String hostName = InetAddress.getLocalHost().getHostName();
            String ip = InetAddress.getLocalHost().getHostAddress();
            HashMap<String, String> paramMap = new HashMap<>();
            paramMap.put("${clusterId}", String.valueOf(clusterId));
            paramMap.put("${host}", hostName);
            paramMap.put("${ip}", ip);
            paramMap.put("${user}", "root");
            paramMap.put("${myid}", String.valueOf(myid));
            logger.info("Start to configure service role {}", serviceRoleName);
            for (Generators generators : configFileMap.keySet()) {
                List<ServiceConfig> configs = configFileMap.get(generators);
                String dataDir = "";
                Iterator<ServiceConfig> iterator = configs.iterator();
                ArrayList<ServiceConfig> customConfList = new ArrayList<>();
                while (iterator.hasNext()) {
                    ServiceConfig config = iterator.next();
                    if (StringUtils.isNotBlank(config.getType())) {
                        switch (config.getType()) {
                            case Constants.INPUT:
                                String value = PlaceholderUtils.replacePlaceholders((String) config.getValue(),
                                        paramMap, Constants.REGEX_VARIABLE);
                                config.setValue(value);
                                break;
                            case Constants.MULTIPLE:
                                conventToStr(config);
                                break;
                            default:
                                break;
                        }
                    }
                    if (Constants.PATH.equals(config.getConfigType())) {
                        createPath(config, runAs);
                    }
                    if (Constants.MV_PATH.equals(config.getConfigType())) {
                        movePath(config, runAs);
                    }
                    if (Constants.CUSTOM.equals(config.getConfigType())) {
                        addToCustomList(iterator, customConfList, config);
                    }
                    if (config.getValue() instanceof Boolean || config.getValue() instanceof Integer) {
                        logger.info("Convert boolean and integer to string");
                        config.setValue(config.getValue().toString());
                    }
                    
                    if ("dataDir".equals(config.getName())) {
                        logger.info("Find dataDir : {}", config.getValue());
                        dataDir = (String) config.getValue();
                    }
                    if ("TrinoCoordinator".equals(serviceRoleName) && "coordinator".equals(config.getName())) {
                        logger.info("Start config trino coordinator");
                        config.setValue("true");
                        ServiceConfig serviceConfig = new ServiceConfig();
                        serviceConfig.setName("node-scheduler.include-coordinator");
                        serviceConfig.setValue("false");
                        customConfList.add(serviceConfig);
                    }
                    if ("fe_priority_networks".equals(config.getName())
                            || "be_priority_networks".equals(config.getName())) {
                        config.setName("priority_networks");
                    }
                    
                    if ("KyuubiServer".equals(serviceRoleName) && "sparkHome".equals(config.getName())) {
                        // add hive-site.xml link in kerberos module
                        final String targetPath =
                                Constants.INSTALL_PATH + File.separator + decompressPackageName + "/conf/hive-site.xml";
                        if (!FileUtil.exist(targetPath)) {
                            logger.info("Add hive-site.xml link");
                            ExecResult result = ShellUtils
                                    .exceShell("ln -s " + config.getValue() + "/conf/hive-site.xml " + targetPath);
                            if (!result.getExecResult()) {
                                logger.warn("Add hive-site.xml link failed,msg: " + result.getExecErrOut());
                            }
                        }
                    }
                }
                
                if (Objects.nonNull(myid) && StringUtils.isNotBlank(dataDir)) {
                    FileUtil.writeUtf8String(myid + "", dataDir + Constants.SLASH + "myid");
                }
                
                if ("node.properties".equals(generators.getFilename())) {
                    ServiceConfig serviceConfig = new ServiceConfig();
                    serviceConfig.setName("node.id");
                    serviceConfig.setValue(IdUtil.simpleUUID());
                    customConfList.add(serviceConfig);
                }
                if ("Grafana".equals(serviceRoleName)) {
                    ServiceConfig clusterIdConfig = new ServiceConfig();
                    clusterIdConfig.setName("clusterId");
                    clusterIdConfig.setValue(String.valueOf(clusterId));
                    clusterIdConfig.setConfigType("map");
                    customConfList.add(clusterIdConfig);
                }
                configs.addAll(customConfList);
                if (!configs.isEmpty()) {
                    // extra app, package: META, templates
                    File extTemplateDir =
                            new File(Constants.INSTALL_PATH + File.separator + decompressPackageName, "templates");
                    if (extTemplateDir.exists() && extTemplateDir.isDirectory()) {
                        // 3rd app, load ext templates
                        logger.info("Add ext app template path: {} to loader path.", extTemplateDir.getAbsolutePath());
                        FreemakerUtils.generateConfigFile(generators, configs, decompressPackageName,
                                extTemplateDir.getAbsolutePath());
                    } else {
                        FreemakerUtils.generateConfigFile(generators, configs, decompressPackageName);
                    }
                } else if (!generators.getFilename().endsWith(SH)) {
                    String packagePath =
                            Constants.INSTALL_PATH + Constants.SLASH + decompressPackageName + Constants.SLASH;
                    String outputFile =
                            packagePath + generators.getOutputDirectory() + Constants.SLASH + generators.getFilename();
                    FileUtil.writeUtf8String("", outputFile);
                }
                execResult.setExecOut("configure success");
                logger.info("configure success");
            }
            if (RANGER_ADMIN.equals(serviceRoleName) && !setupRangerAdmin(decompressPackageName)) {
                return execResult;
            }
            execResult.setExecResult(true);
        } catch (Exception e) {
            execResult.setExecErrOut(e.getMessage());
            logger.error("load app config template error!", e);
        }
        return execResult;
    }
    
    private boolean setupRangerAdmin(String decompressPackageName) {
        logger.info("start to execute ranger admin setup.sh");
        ArrayList<String> commands = new ArrayList<>();
        commands.add(Constants.INSTALL_PATH + Constants.SLASH + decompressPackageName + Constants.SLASH + "setup.sh");
        ExecResult execResult = ShellUtils
                .execWithStatus(Constants.INSTALL_PATH + Constants.SLASH + decompressPackageName, commands, 300L);
        
        ArrayList<String> globalCommand = new ArrayList<>();
        globalCommand.add(
                Constants.INSTALL_PATH + Constants.SLASH + decompressPackageName + Constants.SLASH + "set_globals.sh");
        ShellUtils.execWithStatus(Constants.INSTALL_PATH + Constants.SLASH + decompressPackageName, globalCommand,
                300L, logger);
        if (execResult.getExecResult()) {
            logger.info("ranger admin setup success");
            return true;
        }
        logger.info("ranger admin setup failed");
        return false;
    }
    
    private void createPath(ServiceConfig config, RunAs runAs) {
        String path = (String) config.getValue();
        if (StringUtils.isNotBlank(config.getSeparator()) && path.contains(config.getSeparator())) {
            for (String dir : path.split(config.getSeparator())) {
                mkdir(dir, runAs);
            }
        } else {
            mkdir(path, runAs);
        }
    }
    
    private void movePath(ServiceConfig config, RunAs runAs) {
        String oldPath = (String) config.getDefaultValue();
        String newPath = (String) config.getValue();
        if (FileUtil.exist(oldPath) && !FileUtil.exist(newPath)) {
            if (StringUtils.isNotBlank(config.getSeparator()) && newPath.contains(config.getSeparator())) {
                for (String dir : newPath.split(config.getSeparator())) {
                    mkdir(dir, runAs);
                }
            } else {
                mkdir(newPath, runAs);
            }
            FileUtil.move(new File(oldPath), new File(newPath), false);
            logger.info("move path {} to {}", oldPath, newPath);
        }
    }
    
    private void addToCustomList(Iterator<ServiceConfig> iterator, ArrayList<ServiceConfig> customConfList,
                                 ServiceConfig config) {
        List<JSONObject> list = (List<JSONObject>) config.getValue();
        iterator.remove();
        for (JSONObject json : list) {
            if (Objects.nonNull(json)) {
                Set<String> set = json.keySet();
                for (String key : set) {
                    if (StringUtils.isNotBlank(key)) {
                        ServiceConfig serviceConfig = new ServiceConfig();
                        serviceConfig.setName(key);
                        serviceConfig.setValue(json.get(key));
                        customConfList.add(serviceConfig);
                    }
                }
            }
        }
    }
    
    private String conventToStr(ServiceConfig config) {
        JSONArray value = (JSONArray) config.getValue();
        List<String> strs = value.toJavaList(String.class);
        logger.info("size is :{}", strs.size());
        String joinValue = String.join(config.getSeparator(), strs);
        config.setValue(joinValue);
        logger.info("config set value to {}", config.getValue());
        return joinValue;
    }
    
    private void mkdir(String path, RunAs runAs) {
        if (!FileUtil.exist(path)) {
            logger.info("create file path {}", path);
            FileUtil.mkdir(path);
            ShellUtils.addChmod(path, "775");
            if (Objects.nonNull(runAs)) {
                ShellUtils.addChown(path, runAs.getUser(), runAs.getGroup());
            }
        }
    }
}
