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

package com.datasophon.api.utils;

import com.datasophon.api.load.GlobalVariables;
import com.datasophon.api.load.ServiceConfigMap;
import com.datasophon.api.master.ActorUtils;
import com.datasophon.api.master.CancelCommandMap;
import com.datasophon.api.master.MasterServiceActor;
import com.datasophon.api.master.ServiceCommandActor;
import com.datasophon.api.master.ServiceExecuteResultActor;
import com.datasophon.api.master.handler.service.ServiceConfigureAsyncHandler;
import com.datasophon.api.master.handler.service.ServiceConfigureHandler;
import com.datasophon.api.master.handler.service.ServiceHandler;
import com.datasophon.api.master.handler.service.ServiceInstallHandler;
import com.datasophon.api.master.handler.service.ServiceStartHandler;
import com.datasophon.api.master.handler.service.ServiceStopHandler;
import com.datasophon.api.service.ClusterAlertHistoryService;
import com.datasophon.api.service.ClusterInfoService;
import com.datasophon.api.service.ClusterServiceCommandHostCommandService;
import com.datasophon.api.service.ClusterServiceInstanceConfigService;
import com.datasophon.api.service.ClusterServiceInstanceRoleGroupService;
import com.datasophon.api.service.ClusterServiceInstanceService;
import com.datasophon.api.service.ClusterServiceRoleInstanceService;
import com.datasophon.api.service.ClusterServiceRoleInstanceWebuisService;
import com.datasophon.api.service.ClusterVariableService;
import com.datasophon.api.service.ClusterZkService;
import com.datasophon.api.service.FrameServiceService;
import com.datasophon.api.service.host.ClusterHostService;
import com.datasophon.common.Constants;
import com.datasophon.common.cache.CacheUtils;
import com.datasophon.common.command.ExecuteCmdCommand;
import com.datasophon.common.command.ExecuteServiceRoleCommand;
import com.datasophon.common.command.FileOperateCommand;
import com.datasophon.common.enums.CommandType;
import com.datasophon.common.enums.ServiceExecuteState;
import com.datasophon.common.enums.ServiceRoleType;
import com.datasophon.common.model.DAGGraph;
import com.datasophon.common.model.ExternalLink;
import com.datasophon.common.model.Generators;
import com.datasophon.common.model.ServiceConfig;
import com.datasophon.common.model.ServiceExecuteResultMessage;
import com.datasophon.common.model.ServiceNode;
import com.datasophon.common.model.ServiceRoleInfo;
import com.datasophon.common.model.StartWorkerMessage;
import com.datasophon.common.model.UpdateCommandHostMessage;
import com.datasophon.common.utils.ExecResult;
import com.datasophon.common.utils.HostUtils;
import com.datasophon.common.utils.PlaceholderUtils;
import com.datasophon.common.utils.PropertyUtils;
import com.datasophon.dao.entity.ClusterAlertHistory;
import com.datasophon.dao.entity.ClusterHostDO;
import com.datasophon.dao.entity.ClusterInfoEntity;
import com.datasophon.dao.entity.ClusterServiceCommandEntity;
import com.datasophon.dao.entity.ClusterServiceCommandHostCommandEntity;
import com.datasophon.dao.entity.ClusterServiceCommandHostEntity;
import com.datasophon.dao.entity.ClusterServiceInstanceConfigEntity;
import com.datasophon.dao.entity.ClusterServiceInstanceEntity;
import com.datasophon.dao.entity.ClusterServiceInstanceRoleGroup;
import com.datasophon.dao.entity.ClusterServiceRoleGroupConfig;
import com.datasophon.dao.entity.ClusterServiceRoleInstanceEntity;
import com.datasophon.dao.entity.ClusterServiceRoleInstanceWebuis;
import com.datasophon.dao.entity.ClusterVariable;
import com.datasophon.dao.entity.ClusterZk;
import com.datasophon.dao.entity.FrameServiceEntity;
import com.datasophon.dao.enums.AlertLevel;
import com.datasophon.dao.enums.CommandState;
import com.datasophon.dao.enums.NeedRestart;
import com.datasophon.dao.enums.RoleType;
import com.datasophon.dao.enums.ServiceRoleState;
import com.datasophon.dao.enums.ServiceState;
import com.datasophon.domain.host.enums.HostState;
import com.datasophon.domain.host.enums.MANAGED;

import org.apache.commons.lang3.StringUtils;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import akka.util.Timeout;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.crypto.SecureUtil;

public class ProcessUtils {
    
    private static final Logger logger = LoggerFactory.getLogger(ProcessUtils.class);
    
    public static void saveServiceInstallInfo(ServiceRoleInfo serviceRoleInfo) {
        ClusterServiceInstanceService serviceInstanceService =
                SpringTool.getApplicationContext().getBean(ClusterServiceInstanceService.class);
        ClusterServiceInstanceConfigService serviceInstanceConfigService =
                SpringTool.getApplicationContext().getBean(ClusterServiceInstanceConfigService.class);
        ClusterServiceRoleInstanceService serviceRoleInstanceService =
                SpringTool.getApplicationContext().getBean(ClusterServiceRoleInstanceService.class);
        ClusterInfoService clusterInfoService = SpringTool.getApplicationContext().getBean(ClusterInfoService.class);
        ClusterServiceRoleInstanceWebuisService webuisService =
                SpringTool.getApplicationContext().getBean(ClusterServiceRoleInstanceWebuisService.class);
        ClusterServiceInstanceRoleGroupService roleGroupService =
                SpringTool.getApplicationContext().getBean(ClusterServiceInstanceRoleGroupService.class);
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(serviceRoleInfo.getClusterId());
        ClusterServiceInstanceEntity clusterServiceInstance =
                serviceInstanceService.getServiceInstanceByClusterIdAndServiceName(serviceRoleInfo.getClusterId(),
                        serviceRoleInfo.getParentName());
        if (Objects.isNull(clusterServiceInstance)) {
            clusterServiceInstance = new ClusterServiceInstanceEntity();
            clusterServiceInstance.setClusterId(serviceRoleInfo.getClusterId());
            clusterServiceInstance.setServiceName(serviceRoleInfo.getParentName());
            clusterServiceInstance.setServiceState(ServiceState.RUNNING);
            clusterServiceInstance.setCreateTime(new Date());
            clusterServiceInstance.setUpdateTime(new Date());
            serviceInstanceService.save(clusterServiceInstance);
            // save config
            List<ServiceConfig> list = ServiceConfigMap.get(clusterInfo.getClusterCode() + Constants.UNDERLINE
                    + serviceRoleInfo.getParentName() + Constants.CONFIG);
            String config = JSON.toJSONString(list);
            ClusterServiceInstanceConfigEntity clusterServiceInstanceConfig = new ClusterServiceInstanceConfigEntity();
            clusterServiceInstanceConfig.setClusterId(serviceRoleInfo.getClusterId());
            clusterServiceInstanceConfig.setServiceId(clusterServiceInstance.getId());
            clusterServiceInstanceConfig.setConfigJson(config);
            clusterServiceInstanceConfig.setConfigJsonMd5(SecureUtil.md5(config));
            clusterServiceInstanceConfig.setConfigVersion(1);
            clusterServiceInstanceConfig.setCreateTime(new Date());
            clusterServiceInstanceConfig.setUpdateTime(new Date());
            serviceInstanceConfigService.save(clusterServiceInstanceConfig);
        } else {
            clusterServiceInstance.setServiceState(ServiceState.RUNNING);
            clusterServiceInstance.setServiceStateCode(ServiceState.RUNNING.getValue());
            serviceInstanceService.updateById(clusterServiceInstance);
        }
        Integer roleGroupId = (Integer) CacheUtils.get("UseRoleGroup_" + clusterServiceInstance.getId());
        ClusterServiceInstanceRoleGroup roleGroup = roleGroupService.getById(roleGroupId);
        // save role instance
        ClusterServiceRoleInstanceEntity roleInstanceEntity = serviceRoleInstanceService
                .getOneServiceRole(serviceRoleInfo.getName(), serviceRoleInfo.getHostname(), clusterInfo.getId());
        if (Objects.isNull(roleInstanceEntity)) {
            ClusterServiceRoleInstanceEntity roleInstance = new ClusterServiceRoleInstanceEntity();
            roleInstance.setServiceId(clusterServiceInstance.getId());
            roleInstance.setRoleType(CommonUtils.convertRoleType(serviceRoleInfo.getRoleType().getName()));
            roleInstance.setCreateTime(new Date());
            roleInstance.setHostname(serviceRoleInfo.getHostname());
            roleInstance.setClusterId(serviceRoleInfo.getClusterId());
            roleInstance.setServiceRoleName(serviceRoleInfo.getName());
            roleInstance.setServiceRoleState(ServiceRoleState.RUNNING);
            roleInstance.setUpdateTime(new Date());
            roleInstance.setServiceName(serviceRoleInfo.getParentName());
            roleInstance.setRoleGroupId(roleGroup.getId());
            roleInstance.setNeedRestart(NeedRestart.NO);
            serviceRoleInstanceService.save(roleInstance);
            if (Constants.ZKSERVER.equalsIgnoreCase(roleInstance.getServiceRoleName())) {
                ClusterZkService clusterZkService = SpringTool.getApplicationContext().getBean(ClusterZkService.class);
                ClusterZk clusterZk = new ClusterZk();
                clusterZk.setMyid((Integer) CacheUtils.get("zkserver_" + serviceRoleInfo.getHostname()));
                clusterZk.setClusterId(serviceRoleInfo.getClusterId());
                clusterZk.setZkServer(roleInstance.getHostname());
                clusterZkService.save(clusterZk);
            }
            
            if (Objects.nonNull(serviceRoleInfo.getExternalLink())) {
                ExternalLink externalLink = serviceRoleInfo.getExternalLink();
                ClusterServiceRoleInstanceWebuis webui = webuisService.getRoleInstanceWebUi(roleInstance.getId());
                if (Objects.nonNull(webui)) {
                    logger.info("web ui already exists");
                } else {
                    Map<String, String> globalVariables = GlobalVariables.get(clusterInfo.getId());
                    globalVariables.put("${host}", serviceRoleInfo.getHostname());
                    String url = PlaceholderUtils.replacePlaceholders(externalLink.getUrl(), globalVariables,
                            Constants.REGEX_VARIABLE);
                    ClusterServiceRoleInstanceWebuis webuis = new ClusterServiceRoleInstanceWebuis();
                    webuis.setWebUrl(url);
                    webuis.setServiceInstanceId(clusterServiceInstance.getId());
                    webuis.setServiceRoleInstanceId(roleInstance.getId());
                    webuis.setName(externalLink.getName() + "(" + serviceRoleInfo.getHostname() + ")");
                    webuisService.save(webuis);
                    globalVariables.remove("${host}");
                }
            }
        }
    }
    public static void saveHostInstallInfo(StartWorkerMessage message, String clusterCode,
                                           ClusterHostService clusterHostService) {
        ClusterInfoService clusterInfoService = SpringTool.getApplicationContext().getBean(ClusterInfoService.class);
        ClusterHostDO clusterHostDO = new ClusterHostDO();
        BeanUtil.copyProperties(message, clusterHostDO);
        ClusterInfoEntity cluster = clusterInfoService.getClusterByClusterCode(clusterCode);
        clusterHostDO.setClusterId(cluster.getId());
        clusterHostDO.setCheckTime(new Date());
        clusterHostDO.setRack("/default-rack");
        clusterHostDO.setNodeLabel("default");
        clusterHostDO.setCreateTime(new Date());
        clusterHostDO.setIp(HostUtils.getIp(message.getHostname()));
        clusterHostDO.setHostState(HostState.RUNNING);
        clusterHostDO.setManaged(MANAGED.YES);
        clusterHostService.save(clusterHostDO);
    }
    public static void updateCommandStateToFailed(List<String> commandIds) {
        for (String commandId : commandIds) {
            logger.info("command id is {}", commandId);
            // cancel worker and sub node
            ClusterServiceCommandHostCommandService service =
                    SpringTool.getApplicationContext().getBean(ClusterServiceCommandHostCommandService.class);
            ActorRef commandActor = ActorUtils.getLocalActor(ServiceCommandActor.class, "commandActor");
            List<ClusterServiceCommandHostCommandEntity> hostCommandList =
                    service.getHostCommandListByCommandId(commandId);
            for (ClusterServiceCommandHostCommandEntity hostCommandEntity : hostCommandList) {
                if (hostCommandEntity.getCommandState() == CommandState.RUNNING) {
                    logger.info("{} host command  set to cancel", hostCommandEntity.getCommandName());
                    CancelCommandMap.put(hostCommandEntity.getHostCommandId(), hostCommandEntity.getCommandName());
                    
                    hostCommandEntity.setCommandState(CommandState.CANCEL);
                    hostCommandEntity.setCommandProgress(100);
                    service.updateByHostCommandId(hostCommandEntity);
                    UpdateCommandHostMessage message = new UpdateCommandHostMessage();
                    message.setCommandId(commandId);
                    message.setCommandHostId(hostCommandEntity.getCommandHostId());
                    message.setHostname(hostCommandEntity.getHostname());
                    if (hostCommandEntity.getServiceRoleType() == RoleType.MASTER) {
                        message.setServiceRoleType(ServiceRoleType.MASTER);
                    } else {
                        message.setServiceRoleType(ServiceRoleType.WORKER);
                    }
                    ActorUtils.actorSystem.scheduler().scheduleOnce(
                            FiniteDuration.apply(3L, TimeUnit.SECONDS),
                            commandActor,
                            message,
                            ActorUtils.actorSystem.dispatcher(),
                            ActorRef.noSender());
                }
            }
        }
    }
    public static void tellCommandActorResult(String serviceName, ExecuteServiceRoleCommand executeServiceRoleCommand,
                                              ServiceExecuteState state) {
        ActorRef serviceExecuteResultActor = ActorUtils.getLocalActor(ServiceExecuteResultActor.class,
                ActorUtils.getActorRefName(ServiceExecuteResultActor.class));
        ServiceExecuteResultMessage serviceExecuteResultMessage = new ServiceExecuteResultMessage();
        serviceExecuteResultMessage.setServiceExecuteState(state);
        serviceExecuteResultMessage.setDag(executeServiceRoleCommand.getDag());
        serviceExecuteResultMessage.setServiceName(serviceName);
        serviceExecuteResultMessage.setClusterCode(executeServiceRoleCommand.getClusterCode());
        serviceExecuteResultMessage.setServiceRoleType(executeServiceRoleCommand.getServiceRoleType());
        serviceExecuteResultMessage.setCommandType(executeServiceRoleCommand.getCommandType());
        serviceExecuteResultMessage.setDag(executeServiceRoleCommand.getDag());
        serviceExecuteResultMessage.setClusterId(executeServiceRoleCommand.getClusterId());
        serviceExecuteResultMessage.setActiveTaskList(executeServiceRoleCommand.getActiveTaskList());
        serviceExecuteResultMessage.setErrorTaskList(executeServiceRoleCommand.getErrorTaskList());
        serviceExecuteResultMessage.setReadyToSubmitTaskList(executeServiceRoleCommand.getReadyToSubmitTaskList());
        serviceExecuteResultMessage.setCompleteTaskList(executeServiceRoleCommand.getCompleteTaskList());
        serviceExecuteResultActor.tell(serviceExecuteResultMessage, ActorRef.noSender());
    }
    public static ClusterServiceCommandHostCommandEntity handleCommandResult(String hostCommandId, Boolean execResult,
                                                                             String execOut) {
        ClusterServiceCommandHostCommandService service =
                SpringTool.getApplicationContext().getBean(ClusterServiceCommandHostCommandService.class);
        ClusterServiceCommandHostCommandEntity hostCommand = service.getByHostCommandId(hostCommandId);
        hostCommand.setCommandProgress(100);
        if (execResult) {
            hostCommand.setCommandState(CommandState.SUCCESS);
            hostCommand.setResultMsg("success");
            logger.info("{} in {} success", hostCommand.getCommandName(), hostCommand.getHostname());
        } else {
            hostCommand.setCommandState(CommandState.FAILED);
            hostCommand.setResultMsg(execOut);
            logger.info("{} in {} failed", hostCommand.getCommandName(), hostCommand.getHostname());
        }
        service.updateByHostCommandId(hostCommand);
        // 更新command host进度
        // 更新command进度
        UpdateCommandHostMessage message = new UpdateCommandHostMessage();
        message.setExecResult(execResult);
        message.setCommandId(hostCommand.getCommandId());
        message.setCommandHostId(hostCommand.getCommandHostId());
        message.setHostname(hostCommand.getHostname());
        if (hostCommand.getServiceRoleType() == RoleType.MASTER) {
            message.setServiceRoleType(ServiceRoleType.MASTER);
        } else {
            message.setServiceRoleType(ServiceRoleType.WORKER);
        }
        ActorRef commandActor = ActorUtils.getLocalActor(ServiceCommandActor.class, "commandActor");
        ActorUtils.actorSystem.scheduler().scheduleOnce(FiniteDuration.apply(
                1L, TimeUnit.SECONDS),
                commandActor, message,
                ActorUtils.actorSystem.dispatcher(),
                ActorRef.noSender());
        return hostCommand;
    }
    
    public static void buildExecuteServiceRoleCommand(
                                                      Integer clusterId,
                                                      CommandType commandType,
                                                      String clusterCode,
                                                      DAGGraph<String, ServiceNode, String> dag,
                                                      Map<String, ServiceExecuteState> activeTaskList,
                                                      Map<String, String> errorTaskList,
                                                      Map<String, String> readyToSubmitTaskList,
                                                      Map<String, String> completeTaskList,
                                                      String node,
                                                      List<ServiceRoleInfo> masterRoles,
                                                      ServiceRoleInfo workerRole,
                                                      ActorRef serviceActor,
                                                      ServiceRoleType serviceRoleType) {
        ExecuteServiceRoleCommand executeServiceRoleCommand =
                new ExecuteServiceRoleCommand(clusterId, node, masterRoles);
        executeServiceRoleCommand.setServiceRoleType(serviceRoleType);
        executeServiceRoleCommand.setCommandType(commandType);
        executeServiceRoleCommand.setDag(dag);
        executeServiceRoleCommand.setClusterCode(clusterCode);
        executeServiceRoleCommand.setClusterId(clusterId);
        executeServiceRoleCommand.setActiveTaskList(activeTaskList);
        executeServiceRoleCommand.setErrorTaskList(errorTaskList);
        executeServiceRoleCommand.setReadyToSubmitTaskList(readyToSubmitTaskList);
        executeServiceRoleCommand.setCompleteTaskList(completeTaskList);
        executeServiceRoleCommand.setWorkerRole(workerRole);
        serviceActor.tell(executeServiceRoleCommand, ActorRef.noSender());
    }
    
    public static ClusterServiceCommandEntity generateCommandEntity(Integer clusterId, CommandType commandType,
                                                                    String serviceName) {
        ClusterServiceCommandEntity commandEntity = new ClusterServiceCommandEntity();
        String commandId = IdUtil.simpleUUID();
        commandEntity.setCommandId(commandId);
        commandEntity.setClusterId(clusterId);
        commandEntity.setCommandName(commandType.getCommandName(PropertyUtils.getString(Constants.LOCALE_LANGUAGE))
                + Constants.SPACE + serviceName);
        commandEntity.setCommandProgress(0);
        commandEntity.setCommandState(CommandState.RUNNING);
        commandEntity.setCommandType(commandType.getValue());
        commandEntity.setCreateTime(new Date());
        commandEntity.setCreateBy("admin");
        commandEntity.setServiceName(serviceName);
        return commandEntity;
    }
    
    public static ClusterServiceCommandHostEntity generateCommandHostEntity(String commandId, String hostname) {
        ClusterServiceCommandHostEntity commandHost = new ClusterServiceCommandHostEntity();
        String commandHostId = IdUtil.simpleUUID();
        commandHost.setCommandHostId(commandHostId);
        commandHost.setCommandId(commandId);
        commandHost.setHostname(hostname);
        commandHost.setCommandState(CommandState.RUNNING);
        commandHost.setCommandProgress(0);
        commandHost.setCreateTime(new Date());
        
        return commandHost;
    }
    
    public static ClusterServiceCommandHostCommandEntity generateCommandHostCommandEntity(CommandType commandType,
                                                                                          String commandId,
                                                                                          String serviceRoleName,
                                                                                          RoleType serviceRoleType,
                                                                                          ClusterServiceCommandHostEntity commandHost) {
        ClusterServiceCommandHostCommandEntity hostCommand = new ClusterServiceCommandHostCommandEntity();
        String hostCommandId = IdUtil.simpleUUID();
        hostCommand.setHostCommandId(hostCommandId);
        hostCommand.setServiceRoleName(serviceRoleName);
        hostCommand.setCommandHostId(commandHost.getCommandHostId());
        hostCommand.setCommandState(CommandState.RUNNING);
        hostCommand.setCommandProgress(0);
        hostCommand.setHostname(commandHost.getHostname());
        hostCommand.setCommandName(commandType.getCommandName(PropertyUtils.getString(Constants.LOCALE_LANGUAGE))
                + Constants.SPACE + serviceRoleName);
        hostCommand.setCommandId(commandId);
        hostCommand.setCommandType(commandType.getValue());
        hostCommand.setServiceRoleType(serviceRoleType);
        hostCommand.setCreateTime(new Date());
        return hostCommand;
    }
    
    public static void updateServiceRoleState(CommandType commandType, String serviceRoleName, String hostname,
                                              Integer clusterId, ServiceRoleState serviceRoleState) {
        ClusterServiceRoleInstanceService serviceRoleInstanceService =
                SpringTool.getApplicationContext().getBean(ClusterServiceRoleInstanceService.class);
        ClusterServiceRoleInstanceEntity serviceRole =
                serviceRoleInstanceService.getOneServiceRole(serviceRoleName, hostname, clusterId);
        serviceRole.setServiceRoleState(serviceRoleState);
        serviceRole.setServiceRoleStateCode(serviceRoleState.getValue());
        if (commandType != CommandType.STOP_SERVICE) {
            serviceRole.setNeedRestart(NeedRestart.NO);
        }
        serviceRoleInstanceService.updateById(serviceRole);
    }
    
    public static void generateClusterVariable(Map<String, String> globalVariables, Integer clusterId,
                                               String serviceName, String variableName, String value) {
        ClusterVariableService variableService =
                SpringTool.getApplicationContext().getBean(ClusterVariableService.class);
        ClusterVariable clusterVariable = variableService.getVariableByVariableName(variableName, clusterId);
        if (Objects.nonNull(clusterVariable)) {
            logger.info("update variable {} value {} to {}", variableName, clusterVariable.getVariableValue(), value);
            clusterVariable.setServiceName(serviceName);
            clusterVariable.setVariableValue(value);
            variableService.updateById(clusterVariable);
        } else {
            ClusterVariable newClusterVariable = new ClusterVariable();
            newClusterVariable.setClusterId(clusterId);
            newClusterVariable.setServiceName(serviceName);
            newClusterVariable.setVariableName(variableName);
            newClusterVariable.setVariableValue(value);
            variableService.save(newClusterVariable);
        }
        globalVariables.put(variableName, value);
    }
    
    public static void hdfsEcMethond(Integer serviceInstanceId, ClusterServiceRoleInstanceService roleInstanceService,
                                     TreeSet<String> list, String type, String roleName) throws Exception {
        
        List<ClusterServiceRoleInstanceEntity> namenodes = roleInstanceService.lambdaQuery()
                .eq(ClusterServiceRoleInstanceEntity::getServiceId, serviceInstanceId)
                .eq(ClusterServiceRoleInstanceEntity::getServiceRoleName, roleName)
                .list();
        ClusterVariableService variableService =
                SpringTool.getApplicationContext().getBean(ClusterVariableService.class);
        // 更新namenode节点的whitelist白名单或blacklist黑名单，分别是dfs.hosts、dfs.hosts.exclude 对应的配置文件
        for (ClusterServiceRoleInstanceEntity namenode : namenodes) {
            ActorSelection actorSelection = ActorUtils.actorSystem.actorSelection(
                    "akka.tcp://datasophon@" + namenode.getHostname() + ":2552/user/worker/fileOperateActor");
            ActorSelection execCmdActor = ActorUtils.actorSystem.actorSelection(
                    "akka.tcp://datasophon@" + namenode.getHostname() + ":2552/user/worker/executeCmdActor");
            Timeout timeout = new Timeout(Duration.create(180, TimeUnit.SECONDS));
            // DataNode 节点的 hostname 作为文件内容
            FileOperateCommand fileOperateCommand = new FileOperateCommand();
            fileOperateCommand.setLines(list);
            // fileOperateCommand.setPath(Constants.INSTALL_PATH + "/hadoop/etc/hadoop/" + type);
            if ("whitelist".equals(type)) {
                // 优先获取通过页面白名单文件变量配置的路径，而不是写死文件路径
                ClusterVariable clusterVariable = variableService.getVariableByVariableName("${dfs.hosts}", namenode.getClusterId());
                if (!Objects.isNull(clusterVariable) && !StringUtils.isBlank(clusterVariable.getVariableValue())) {
                    logger.info("dfs.hosts value is {}", clusterVariable.getVariableValue());
                    fileOperateCommand.setPath(clusterVariable.getVariableValue());
                } else {
                    fileOperateCommand.setPath(Constants.INSTALL_PATH + "/hadoop/etc/hadoop/" + type);
                }
            } else {
                ClusterVariable clusterVariable = variableService.getVariableByVariableName("${dfs.hosts.exclude}", namenode.getClusterId());
                if (!Objects.isNull(clusterVariable) && !StringUtils.isBlank(clusterVariable.getVariableValue())) {
                    fileOperateCommand.setPath(clusterVariable.getVariableValue());
                } else {
                    fileOperateCommand.setPath(Constants.INSTALL_PATH + "/hadoop/etc/hadoop/" + type);
                }
            }
            Future<Object> future = Patterns.ask(actorSelection, fileOperateCommand, timeout);
            ExecResult fileOperateResult = (ExecResult) Await.result(future, timeout.duration());
            if (Objects.nonNull(fileOperateResult) && fileOperateResult.getExecResult()) {
                logger.info("write {} success in namenode {}", type, namenode.getHostname());
                // 刷新白名单
                ExecuteCmdCommand command = new ExecuteCmdCommand();
                ArrayList<String> commands = new ArrayList<>();
                commands.add(Constants.INSTALL_PATH + "/hadoop/bin/hdfs");
                commands.add("dfsadmin");
                commands.add("-refreshNodes");
                command.setCommands(commands);
                Future<Object> execFuture = Patterns.ask(execCmdActor, command, timeout);
                ExecResult execResult = (ExecResult) Await.result(execFuture, timeout.duration());
                if (execResult.getExecResult()) {
                    logger.info("hdfs dfsadmin -refreshNodes success at {}", namenode.getHostname());
                }
            }
        }
    }
    
    public static void createServiceActor(ClusterInfoEntity clusterInfo) {
        FrameServiceService frameServiceService = SpringTool.getApplicationContext().getBean(FrameServiceService.class);
        
        List<FrameServiceEntity> frameServiceList =
                frameServiceService.getAllFrameServiceByFrameCode(clusterInfo.getClusterFrame());
        for (FrameServiceEntity frameServiceEntity : frameServiceList) {
            // create service actor
            logger.info("create {} actor",
                    clusterInfo.getClusterCode() + "-serviceActor-" + frameServiceEntity.getServiceName());
            ActorUtils.actorSystem.actorOf(Props.create(MasterServiceActor.class)
                    .withDispatcher("my-forkjoin-dispatcher"),
                    clusterInfo.getClusterCode() + "-serviceActor-" + frameServiceEntity.getServiceName());
        }
    }
    
    public static String getExceptionMessage(Exception ex) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream pout = new PrintStream(out);
        ex.printStackTrace(pout);
        String ret = new String(out.toByteArray());
        pout.close();
        try {
            out.close();
        } catch (Exception e) {
        }
        return ret;
    }
    
    public static ExecResult restartService(ServiceRoleInfo serviceRoleInfo, boolean needReConfig) throws Exception {
        ServiceHandler serviceStartHandler = new ServiceStartHandler();
        ServiceHandler serviceStopHandler = new ServiceStopHandler();
        if (needReConfig) {
            ServiceConfigureHandler serviceConfigureHandler = new ServiceConfigureHandler();
            serviceStopHandler.setNext(serviceConfigureHandler);
            serviceConfigureHandler.setNext(serviceStartHandler);
        } else {
            serviceStopHandler.setNext(serviceStartHandler);
        }
        return serviceStopHandler.handlerRequest(serviceRoleInfo);
    }
    
    public static ExecResult startService(ServiceRoleInfo serviceRoleInfo, boolean needReConfig) throws Exception {
        ExecResult execResult;
        if (needReConfig) {
            ServiceConfigureHandler serviceHandler = new ServiceConfigureHandler();
            ServiceHandler serviceStartHandler = new ServiceStartHandler();
            serviceHandler.setNext(serviceStartHandler);
            execResult = serviceHandler.handlerRequest(serviceRoleInfo);
        } else {
            ServiceHandler serviceStartHandler = new ServiceStartHandler();
            execResult = serviceStartHandler.handlerRequest(serviceRoleInfo);
        }
        return execResult;
    }
    
    public static ExecResult startInstallService(ServiceRoleInfo serviceRoleInfo) throws Exception {
        ServiceHandler serviceInstallHandler = new ServiceInstallHandler();
        ServiceHandler serviceConfigureHandler = new ServiceConfigureHandler();
        ServiceHandler serviceStartHandler = new ServiceStartHandler();
        serviceInstallHandler.setNext(serviceConfigureHandler);
        serviceConfigureHandler.setNext(serviceStartHandler);
        ExecResult execResult = serviceInstallHandler.handlerRequest(serviceRoleInfo);
        return execResult;
    }
    
    public static ExecResult configServiceRoleInstance(ClusterInfoEntity clusterInfo,
                                                       Map<Generators, List<ServiceConfig>> configFileMap,
                                                       ClusterServiceRoleInstanceEntity roleInstanceEntity) throws Exception {
        ServiceRoleInfo serviceRoleInfo = new ServiceRoleInfo();
        serviceRoleInfo.setName(roleInstanceEntity.getServiceRoleName());
        serviceRoleInfo.setParentName(roleInstanceEntity.getServiceName());
        serviceRoleInfo.setConfigFileMap(configFileMap);
        serviceRoleInfo
                .setDecompressPackageName(PackageUtils.getServiceDcPackageName(clusterInfo.getClusterFrame(), "YARN"));
        serviceRoleInfo.setHostname(roleInstanceEntity.getHostname());
        ServiceConfigureHandler configureHandler = new ServiceConfigureHandler();
        return configureHandler.handlerRequest(serviceRoleInfo);
    }
    
    public static void asyncConfigServiceRoleInstance(ClusterInfoEntity clusterInfo,
                                                      Map<Generators, List<ServiceConfig>> configFileMap,
                                                      ClusterServiceRoleInstanceEntity roleInstanceEntity,
                                                      OnComplete<Object> onComplete) {
        ServiceRoleInfo serviceRoleInfo = new ServiceRoleInfo();
        serviceRoleInfo.setName(roleInstanceEntity.getServiceRoleName());
        serviceRoleInfo.setParentName(roleInstanceEntity.getServiceName());
        serviceRoleInfo.setConfigFileMap(configFileMap);
        serviceRoleInfo
                .setDecompressPackageName(PackageUtils.getServiceDcPackageName(clusterInfo.getClusterFrame(), "YARN"));
        serviceRoleInfo.setHostname(roleInstanceEntity.getHostname());
        ServiceConfigureAsyncHandler configureAsyncHandler = new ServiceConfigureAsyncHandler(onComplete);
        configureAsyncHandler.handlerRequest(serviceRoleInfo);
    }
    
    /**
     * @param configFileMap
     * @param config
     * @Description: 生成configFileMap
     */
    public static void generateConfigFileMap(Map<Generators, List<ServiceConfig>> configFileMap,
                                             ClusterServiceRoleGroupConfig config, Integer clusterId) {
        Map<JSONObject, JSONArray> map = JSONObject.parseObject(config.getConfigFileJson(), Map.class);
        for (JSONObject fileJson : map.keySet()) {
            Generators generators = fileJson.toJavaObject(Generators.class);
            List<ServiceConfig> serviceConfigs = map.get(fileJson).toJavaList(ServiceConfig.class);
            // replace variable
            replaceVariable(serviceConfigs, clusterId);
            configFileMap.put(generators, serviceConfigs);
        }
    }
    
    private static void replaceVariable(List<ServiceConfig> serviceConfigs, Integer clusterId) {
        Map<String, String> globalVariables = GlobalVariables.get(clusterId);
        for (ServiceConfig serviceConfig : serviceConfigs) {
            if (Constants.INPUT.equals(serviceConfig.getType())) {
                String name = PlaceholderUtils.replacePlaceholders(serviceConfig.getName(), globalVariables,
                        Constants.REGEX_VARIABLE);
                serviceConfig.setName(name);
                
                String value = PlaceholderUtils.replacePlaceholders((String) serviceConfig.getValue(), globalVariables,
                        Constants.REGEX_VARIABLE);
                serviceConfig.setValue(value);
            }
        }
    }
    
    public static List<ServiceConfig> getServiceConfig(ClusterServiceRoleGroupConfig config) {
        return JSONObject.parseArray(config.getConfigJson(), ServiceConfig.class);
    }
    
    public static ServiceConfig createServiceConfig(String configName, Object configValue, String type) {
        ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setName(configName);
        serviceConfig.setLabel(configName);
        serviceConfig.setValue(configValue);
        serviceConfig.setRequired(true);
        serviceConfig.setHidden(false);
        serviceConfig.setType(type);
        return serviceConfig;
    }
    
    public static ClusterInfoEntity getClusterInfo(Integer clusterId) {
        ClusterInfoService clusterInfoService = SpringTool.getApplicationContext().getBean(ClusterInfoService.class);
        return clusterInfoService.getById(clusterId);
    }
    
    /**
     * 并集：左边集合与右边集合合并
     *
     * @param left
     * @param right
     * @return
     */
    public static List<ServiceConfig> addAll(List<ServiceConfig> left, List<ServiceConfig> right) {
        if (left == null) {
            return null;
        }
        if (right == null) {
            return left;
        }
        // 使用LinkedList方便插入和删除
        List<ServiceConfig> res = new LinkedList<>(right);
        Set<String> set = new HashSet<>();
        //
        for (ServiceConfig item : left) {
            set.add(item.getName());
        }
        // 迭代器遍历listA
        for (ServiceConfig item : res) {
            // 如果set中包含id则remove
            if (!set.contains(item.getName())) {
                left.add(item);
            }
        }
        return left;
    }
    
    public static void syncUserGroupToHosts(List<ClusterHostDO> hostList, String groupName, String operate) {
        for (ClusterHostDO hostEntity : hostList) {
            ActorRef execCmdActor = ActorUtils.getRemoteActor(hostEntity.getHostname(), "unixGroupActor");
            ExecuteCmdCommand command = new ExecuteCmdCommand();
            ArrayList<String> commands = new ArrayList<>();
            commands.add(operate);
            commands.add(groupName);
            command.setCommands(commands);
            execCmdActor.tell(command, ActorRef.noSender());
        }
    }
    
    public static Map<String, ServiceConfig> translateToMap(List<ServiceConfig> list) {
        return list.stream()
                .collect(Collectors.toMap(ServiceConfig::getName, serviceConfig -> serviceConfig, (v1, v2) -> v1));
    }
    
    public static void syncUserToHosts(List<ClusterHostDO> hostList, String username, String mainGroup,
                                       String otherGroup, String operate) {
        for (ClusterHostDO hostEntity : hostList) {
            ActorRef execCmdActor = ActorUtils.getRemoteActor(hostEntity.getHostname(), "executeCmdActor");
            ExecuteCmdCommand command = new ExecuteCmdCommand();
            ArrayList<String> commands = new ArrayList<>();
            commands.add(operate);
            commands.add(username);
            if (StringUtils.isNotBlank(mainGroup)) {
                commands.add("-g");
                commands.add(mainGroup);
            }
            if (StringUtils.isNotBlank(otherGroup)) {
                commands.add("-G");
                commands.add(otherGroup);
            }
            command.setCommands(commands);
            execCmdActor.tell(command, ActorRef.noSender());
        }
    }
    
    public static void recoverAlert(ClusterServiceRoleInstanceEntity roleInstanceEntity) {
        ClusterServiceRoleInstanceService roleInstanceService =
                SpringTool.getApplicationContext().getBean(ClusterServiceRoleInstanceService.class);
        ClusterAlertHistoryService alertHistoryService =
                SpringTool.getApplicationContext().getBean(ClusterAlertHistoryService.class);
        ClusterAlertHistory clusterAlertHistory = alertHistoryService.getOne(new QueryWrapper<ClusterAlertHistory>()
                .eq(Constants.ALERT_TARGET_NAME, roleInstanceEntity.getServiceRoleName() + " Survive")
                .eq(Constants.CLUSTER_ID, roleInstanceEntity.getClusterId())
                .eq(Constants.HOSTNAME, roleInstanceEntity.getHostname())
                .eq(Constants.IS_ENABLED, 1));
        if (Objects.nonNull(clusterAlertHistory)) {
            clusterAlertHistory.setIsEnabled(2);
            alertHistoryService.updateById(clusterAlertHistory);
        }
        // update service role instance state
        if (roleInstanceEntity.getServiceRoleState() != ServiceRoleState.RUNNING) {
            roleInstanceEntity.setServiceRoleState(ServiceRoleState.RUNNING);
            roleInstanceService.updateById(roleInstanceEntity);
        }
    }
    
    public static void saveAlert(ClusterServiceRoleInstanceEntity roleInstanceEntity, String alertTargetName,
                                 AlertLevel alertLevel, String alertAdvice) {
        ClusterServiceRoleInstanceService roleInstanceService =
                SpringTool.getApplicationContext().getBean(ClusterServiceRoleInstanceService.class);
        ClusterAlertHistoryService alertHistoryService =
                SpringTool.getApplicationContext().getBean(ClusterAlertHistoryService.class);
        ClusterServiceInstanceService serviceInstanceService =
                SpringTool.getApplicationContext().getBean(ClusterServiceInstanceService.class);
        ClusterAlertHistory clusterAlertHistory = alertHistoryService.getOne(new QueryWrapper<ClusterAlertHistory>()
                .eq(Constants.ALERT_TARGET_NAME, alertTargetName)
                .eq(Constants.CLUSTER_ID, roleInstanceEntity.getClusterId())
                .eq(Constants.HOSTNAME, roleInstanceEntity.getHostname())
                .eq(Constants.IS_ENABLED, 1));
        
        ClusterServiceInstanceEntity serviceInstanceEntity =
                serviceInstanceService.getById(roleInstanceEntity.getServiceId());
        if (Objects.isNull(clusterAlertHistory)) {
            clusterAlertHistory = ClusterAlertHistory.builder()
                    .clusterId(roleInstanceEntity.getClusterId())
                    .alertGroupName(roleInstanceEntity.getServiceName().toLowerCase())
                    .alertTargetName(alertTargetName)
                    .createTime(new Date())
                    .updateTime(new Date())
                    .alertLevel(alertLevel)
                    .alertInfo("")
                    .alertAdvice(alertAdvice)
                    .hostname(roleInstanceEntity.getHostname())
                    .serviceRoleInstanceId(roleInstanceEntity.getId())
                    .serviceInstanceId(roleInstanceEntity.getServiceId())
                    .isEnabled(1)
                    .serviceInstanceId(roleInstanceEntity.getServiceId())
                    .build();
            
            alertHistoryService.save(clusterAlertHistory);
        }
        // update service role instance state
        serviceInstanceEntity.setServiceState(ServiceState.EXISTS_EXCEPTION);
        roleInstanceEntity.setServiceRoleState(ServiceRoleState.STOP);
        if (alertLevel == AlertLevel.WARN) {
            serviceInstanceEntity.setServiceState(ServiceState.EXISTS_ALARM);
            roleInstanceEntity.setServiceRoleState(ServiceRoleState.EXISTS_ALARM);
        }
        serviceInstanceService.updateById(serviceInstanceEntity);
        roleInstanceService.updateById(roleInstanceEntity);
        
    }
    
}
