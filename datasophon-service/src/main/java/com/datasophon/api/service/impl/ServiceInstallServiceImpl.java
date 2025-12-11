/*
 *
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
 *
 */

package com.datasophon.api.service.impl;

import static com.datasophon.common.Constants.META_PATH;

import com.datasophon.api.enums.Status;
import com.datasophon.api.exceptions.ServiceException;
import com.datasophon.api.load.GlobalVariables;
import com.datasophon.api.load.ServiceConfigMap;
import com.datasophon.api.load.ServiceInfoMap;
import com.datasophon.api.load.ServiceRoleMap;
import com.datasophon.api.service.ClusterInfoService;
import com.datasophon.api.service.ClusterServiceCommandHostCommandService;
import com.datasophon.api.service.ClusterServiceCommandService;
import com.datasophon.api.service.ClusterServiceInstanceConfigService;
import com.datasophon.api.service.ClusterServiceInstanceRoleGroupService;
import com.datasophon.api.service.ClusterServiceInstanceService;
import com.datasophon.api.service.ClusterServiceRoleGroupConfigService;
import com.datasophon.api.service.ClusterServiceRoleInstanceService;
import com.datasophon.api.service.ClusterVariableService;
import com.datasophon.api.service.FrameInfoService;
import com.datasophon.api.service.FrameServiceRoleService;
import com.datasophon.api.service.FrameServiceService;
import com.datasophon.api.service.ServiceInstallService;
import com.datasophon.api.service.host.ClusterHostService;
import com.datasophon.api.strategy.ServiceRoleStrategy;
import com.datasophon.api.strategy.ServiceRoleStrategyContext;
import com.datasophon.common.Constants;
import com.datasophon.common.cache.CacheUtils;
import com.datasophon.common.enums.ServiceRoleType;
import com.datasophon.common.model.DAG;
import com.datasophon.common.model.Generators;
import com.datasophon.common.model.HostServiceRoleMapping;
import com.datasophon.common.model.ServiceConfig;
import com.datasophon.common.model.ServiceInfo;
import com.datasophon.common.model.ServiceNode;
import com.datasophon.common.model.ServiceNodeEdge;
import com.datasophon.common.model.ServiceRoleHostMapping;
import com.datasophon.common.model.ServiceRoleInfo;
import com.datasophon.common.utils.CollectionUtils;
import com.datasophon.common.utils.PlaceholderUtils;
import com.datasophon.common.utils.Result;
import com.datasophon.dao.entity.ClusterHostDO;
import com.datasophon.dao.entity.ClusterInfoEntity;
import com.datasophon.dao.entity.ClusterServiceCommandEntity;
import com.datasophon.dao.entity.ClusterServiceCommandHostCommandEntity;
import com.datasophon.dao.entity.ClusterServiceInstanceEntity;
import com.datasophon.dao.entity.ClusterServiceInstanceRoleGroup;
import com.datasophon.dao.entity.ClusterServiceRoleGroupConfig;
import com.datasophon.dao.entity.ClusterServiceRoleInstanceEntity;
import com.datasophon.dao.entity.ClusterVariable;
import com.datasophon.dao.entity.FrameServiceEntity;
import com.datasophon.dao.entity.FrameServiceRoleEntity;
import com.datasophon.dao.enums.NeedRestart;
import com.datasophon.dao.enums.ServiceState;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;

import cn.hutool.core.io.FileUtil;
import cn.hutool.crypto.SecureUtil;

@Service("serviceInstallService")
@Transactional
public class ServiceInstallServiceImpl implements ServiceInstallService {
    
    private static final Logger logger = LoggerFactory.getLogger(ServiceInstallServiceImpl.class);
    
    private static final List<String> MUST_AT_SAME_NODE_BASIC_SERVICE =
            Arrays.asList("Grafana", "AlertManager", "Prometheus");
    
    @Autowired
    private ClusterInfoService clusterInfoService;
    
    @Autowired
    FrameInfoService frameInfoService;
    
    @Autowired
    FrameServiceService frameService;
    
    @Autowired
    FrameServiceRoleService frameServiceRoleService;
    
    @Autowired
    ClusterServiceCommandService commandService;
    
    @Autowired
    private ClusterServiceInstanceService serviceInstanceService;
    
    @Autowired
    private ClusterServiceInstanceConfigService serviceInstanceConfigService;
    
    @Autowired
    private ClusterServiceCommandHostCommandService hostCommandService;
    
    @Autowired
    private ClusterVariableService variableService;
    
    @Autowired
    private ClusterHostService hostService;
    
    @Autowired
    private ClusterServiceInstanceRoleGroupService roleGroupService;
    
    @Autowired
    private ClusterServiceRoleGroupConfigService groupConfigService;
    
    @Autowired
    private ClusterServiceRoleInstanceService roleInstanceService;
    
    public static final String PROMETHEUS = "prometheus";
    
    @Override
    public Result getServiceConfigOption(Integer clusterId, String serviceName) {
        List<ServiceConfig> list = null;
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        
        Map<String, String> globalVariables = GlobalVariables.get(clusterId);
        
        ClusterServiceInstanceEntity serviceInstance =
                serviceInstanceService.getServiceInstanceByClusterIdAndServiceName(
                        clusterId, serviceName);
        if (Objects.nonNull(serviceInstance)) {
            list = listServiceConfigByServiceInstance(serviceInstance);
        } else {
            FrameServiceEntity frameService =
                    this.frameService.getServiceByFrameCodeAndServiceName(
                            clusterInfo.getClusterFrame(), serviceName);
            String serviceConfig = frameService.getServiceConfig();
            serviceConfig =
                    PlaceholderUtils.replacePlaceholders(
                            serviceConfig, globalVariables, Constants.REGEX_VARIABLE);
            
            list = JSONArray.parseArray(serviceConfig, ServiceConfig.class);
        }
        
        ServiceRoleStrategy serviceRoleHandler =
                ServiceRoleStrategyContext.getServiceRoleHandler(serviceName);
        if (Objects.nonNull(serviceRoleHandler)) {
            serviceRoleHandler.getConfig(clusterId, list);
        }
        
        return Result.success(list);
    }
    
    @Override
    public Result saveServiceConfig(
                                    Integer clusterId, String serviceName, List<ServiceConfig> list,
                                    Integer roleGroupId) {
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        ServiceConfigMap.put(
                clusterInfo.getClusterCode() + Constants.UNDERLINE + serviceName + Constants.CONFIG,
                list);
        HashMap<String, ServiceConfig> map = new HashMap<>();
        Map<String, String> globalVariables = GlobalVariables.get(clusterId);
        // handler config
        ServiceRoleStrategy serviceRoleHandler =
                ServiceRoleStrategyContext.getServiceRoleHandler(serviceName);
        if (Objects.nonNull(serviceRoleHandler)) {
            serviceRoleHandler.handlerConfig(clusterId, list, ServiceRoleStrategyContext.getServiceName(serviceName));
        }
        // add variable
        FrameServiceEntity frameServiceEntity =
                frameService.getServiceByFrameCodeAndServiceName(
                        clusterInfo.getClusterFrame(), serviceName);
        Boolean configUpdate = false;
        for (ServiceConfig serviceConfig : list) {
            String configName = serviceConfig.getName();
            String variableName = "${" + configName + "}";
            String variableValue = String.valueOf(serviceConfig.getValue());
            // add to global variable
            if (Constants.INPUT.equals(serviceConfig.getType())) {
                addToGlobalVariable(clusterId, serviceName, variableName, variableValue);
            }
            globalVariables.put(variableName, variableValue);
            map.put(serviceConfig.getName(), serviceConfig);
        }
        // update config-file
        HashMap<Generators, List<ServiceConfig>> configFileMap = new HashMap<>();
        buildConfigFileMap(serviceName, clusterInfo, map, configFileMap);
        if (PROMETHEUS.equals(serviceName.toLowerCase())) {
            logger.info("add worker and node to prometheus");
            // add host node to prometheus
            addHostNodeToPrometheus(clusterId, configFileMap);
        }
        ClusterServiceInstanceEntity serviceInstanceEntity =
                serviceInstanceService.getServiceInstanceByClusterIdAndServiceName(
                        clusterId, serviceName);
        if (Objects.isNull(serviceInstanceEntity)) {
            serviceInstanceEntity = saveServiceInstance(clusterId, serviceName, frameServiceEntity);
            ClusterServiceInstanceRoleGroup clusterServiceInstanceRoleGroup =
                    saveServiceInstanceRoleGroup(clusterId, serviceName, serviceInstanceEntity);
            saveServiceRoleGroupConfig(
                    clusterId, serviceName, list, configFileMap, clusterServiceInstanceRoleGroup);
            CacheUtils.put(
                    "UseRoleGroup_" + serviceInstanceEntity.getId(),
                    clusterServiceInstanceRoleGroup.getId());
        } else {
            configUpdate = isConfigNeedUpdate(serviceInstanceEntity, list);
            ClusterServiceRoleGroupConfig roleGroupConfig;
            if (Objects.isNull(roleGroupId)) {
                ClusterServiceInstanceRoleGroup roleGroup =
                        roleGroupService.getRoleGroupByServiceInstanceId(
                                serviceInstanceEntity.getId());
                roleGroupConfig = groupConfigService.getConfigByRoleGroupId(roleGroup.getId());
            } else {
                roleGroupConfig = groupConfigService.getConfigByRoleGroupId(roleGroupId);
            }
            CacheUtils.put(
                    "UseRoleGroup_" + serviceInstanceEntity.getId(),
                    roleGroupConfig.getRoleGroupId());
            if (configUpdate) {
                ClusterServiceRoleGroupConfig newRoleGroupConfig =
                        new ClusterServiceRoleGroupConfig();
                if (Objects.isNull(roleGroupId)) {
                    ClusterServiceInstanceRoleGroup roleGroup =
                            saveNewRoleGroup(serviceInstanceEntity);
                    newRoleGroupConfig.setConfigVersion(1);
                    newRoleGroupConfig.setRoleGroupId(roleGroup.getId());
                    CacheUtils.put(
                            "UseRoleGroup_" + serviceInstanceEntity.getId(), roleGroup.getId());
                } else {
                    newRoleGroupConfig.setConfigVersion(roleGroupConfig.getConfigVersion() + 1);
                    newRoleGroupConfig.setRoleGroupId(roleGroupConfig.getRoleGroupId());
                    roleInstanceService.updateToNeedRestart(roleGroupId);
                    roleGroupService.updateToNeedRestart(roleGroupId);
                    serviceInstanceEntity.setNeedRestart(NeedRestart.YES);
                }
                newRoleGroupConfig.setClusterId(clusterId);
                newRoleGroupConfig.setCreateTime(new Date());
                newRoleGroupConfig.setUpdateTime(new Date());
                newRoleGroupConfig.setServiceName(serviceInstanceEntity.getServiceName());
                buildConfig(list, configFileMap, newRoleGroupConfig);
                groupConfigService.save(newRoleGroupConfig);
            }
            // update service instance
            serviceInstanceEntity.setUpdateTime(new Date());
            serviceInstanceEntity.setLabel(frameServiceEntity.getLabel());
            serviceInstanceService.updateById(serviceInstanceEntity);
        }
        return Result.success();
    }
    
    @Override
    public Result saveServiceRoleHostMapping(Integer clusterId, List<ServiceRoleHostMapping> list) {
        
        checkOnSameNode(clusterId, list);
        
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        String hostMapKey =
                clusterInfo.getClusterCode()
                        + Constants.UNDERLINE
                        + Constants.SERVICE_ROLE_HOST_MAPPING;
        HashMap<String, List<String>> map = new HashMap<>();
        if (CacheUtils.constainsKey(hostMapKey)) {
            map = (HashMap<String, List<String>>) CacheUtils.get(hostMapKey);
        }
        
        for (ServiceRoleHostMapping serviceRoleHostMapping : list) {
            serviceValidation(serviceRoleHostMapping);
            
            map.put(serviceRoleHostMapping.getServiceRole(), serviceRoleHostMapping.getHosts());
            
            ServiceRoleStrategy serviceRoleHandler =
                    ServiceRoleStrategyContext.getServiceRoleHandler(
                            serviceRoleHostMapping.getServiceRole());
            String serviceName = ServiceRoleStrategyContext.getServiceName(
                    serviceRoleHostMapping.getServiceRole());
            if (Objects.nonNull(serviceRoleHandler)) {
                serviceRoleHandler.handler(clusterId, serviceRoleHostMapping.getHosts(), serviceName);
            }
        }
        
        CacheUtils.put(
                clusterInfo.getClusterCode()
                        + Constants.UNDERLINE
                        + Constants.SERVICE_ROLE_HOST_MAPPING,
                map);
        return Result.success();
    }
    
    @Override
    public Result saveHostServiceRoleMapping(Integer clusterId, List<HostServiceRoleMapping> list) {
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        HashMap<String, List<String>> map = new HashMap<>();
        for (HostServiceRoleMapping hostServiceRoleMapping : list) {
            map.put(hostServiceRoleMapping.getHost(), hostServiceRoleMapping.getServiceRoles());
        }
        CacheUtils.put(
                clusterInfo.getClusterCode()
                        + Constants.UNDERLINE
                        + Constants.HOST_SERVICE_ROLE_MAPPING,
                map);
        return Result.success();
    }
    
    @Override
    public Result getServiceRoleDeployOverview(Integer clusterId) {
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        HashMap<String, List<String>> map =
                (HashMap<String, List<String>>) CacheUtils.get(
                        clusterInfo.getClusterCode()
                                + Constants.UNDERLINE
                                + Constants.SERVICE_ROLE_HOST_MAPPING);
        return Result.success(map);
    }
    
    /**
     * @param clusterId
     * @param commandIds
     * @return
     */
    @Override
    public Result startInstallService(Integer clusterId, List<String> commandIds) {
        Collection<ClusterServiceCommandEntity> commands = commandService.listByIds(commandIds);
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        DAG<String, ServiceNode, ServiceNodeEdge> dag = new DAG<>();
        for (ClusterServiceCommandEntity command : commands) {
            List<ClusterServiceCommandHostCommandEntity> commandHostList =
                    hostCommandService.getHostCommandListByCommandId(command.getCommandId());
            List<ServiceRoleInfo> masterRoles = new ArrayList<>();
            List<ServiceRoleInfo> elseRoles = new ArrayList<>();
            ServiceNode serviceNode = new ServiceNode();
            String serviceKey =
                    clusterInfo.getClusterFrame() + Constants.UNDERLINE + command.getServiceName();
            ServiceInfo serviceInfo = ServiceInfoMap.get(serviceKey);
            for (ClusterServiceCommandHostCommandEntity hostCommand : commandHostList) {
                String key =
                        clusterInfo.getClusterFrame()
                                + Constants.UNDERLINE
                                + command.getServiceName()
                                + Constants.UNDERLINE
                                + hostCommand.getServiceRoleName();
                ServiceRoleInfo serviceRoleInfo = ServiceRoleMap.get(key);
                serviceRoleInfo.setHostname(hostCommand.getHostname());
                serviceRoleInfo.setHostCommandId(hostCommand.getHostCommandId());
                serviceRoleInfo.setClusterId(clusterId);
                serviceRoleInfo.setParentName(command.getServiceName());
                if (ServiceRoleType.MASTER.equals(serviceRoleInfo.getRoleType())) {
                    masterRoles.add(serviceRoleInfo);
                } else {
                    elseRoles.add(serviceRoleInfo);
                }
            }
            serviceNode.setMasterRoles(masterRoles);
            serviceNode.setElseRoles(elseRoles);
            dag.addNode(command.getServiceName(), serviceNode);
            if (serviceInfo.getDependencies().size() > 0) {
                for (String dependency : serviceInfo.getDependencies()) {
                    dag.addEdge(dependency, command.getServiceName());
                }
            }
        }
        return Result.success();
    }
    
    @Override
    public void downloadPackage(String packageName, HttpServletResponse response) throws IOException {
        FileInputStream inputStream = null;
        OutputStream out = null;
        // 通过文件路径获得File对象
        File file = new File(Constants.MASTER_MANAGE_PACKAGE_PATH + Constants.SLASH + packageName);
        
        inputStream = new FileInputStream(file);
        
        response.reset();
        response.setContentType("application/octet-stream");
        response.addHeader("Content-Length", "" + file.length());
        // 支持中文名称文件,需要对header进行单独设置，不然下载的文件名会出现乱码或者无法显示的情况
        // 设置响应头，控制浏览器下载该文件
        response.setHeader("Content-Disposition", "attachment;filename=" + packageName);
        // 通过response获取ServletOutputStream对象(out)
        out = response.getOutputStream();
        int length = 0;
        byte[] buffer = new byte[1024];
        while ((length = inputStream.read(buffer)) != -1) {
            // 4.写到输出流(out)中
            out.write(buffer, 0, length);
        }
        inputStream.close();
        out.flush();
        out.close();
    }
    
    @Override
    public void downloadResource(String frameCode, String serviceRoleName, String resource,
                                 HttpServletResponse response) throws IOException {
        String metaPath = FileUtil.getAbsolutePath(META_PATH);
        FrameServiceRoleEntity entity =
                frameServiceRoleService.getServiceRoleByFrameCodeAndServiceRoleName(frameCode, serviceRoleName);
        ServiceRoleInfo roleInfo = JSONObject.parseObject(entity.getServiceRoleJson(), ServiceRoleInfo.class);
        
        OutputStream out = null;
        // 通过文件路径获得File对象
        File file = new File(metaPath + Constants.SLASH + frameCode + Constants.SLASH + roleInfo.getParentName()
                + Constants.SLASH + resource);
        try (FileInputStream fis = new FileInputStream(file)) {
            response.reset();
            response.setContentType("application/octet-stream");
            response.addHeader("Content-Length", "" + file.length());
            // 支持中文名称文件,需要对header进行单独设置，不然下载的文件名会出现乱码或者无法显示的情况
            // 设置响应头，控制浏览器下载该文件
            response.setHeader("Content-Disposition", "attachment;filename=" + file.getName());
            // 通过response获取ServletOutputStream对象(out)
            out = response.getOutputStream();
            int length = 0;
            byte[] buffer = new byte[1024];
            while ((length = fis.read(buffer)) != -1) {
                // 4.写到输出流(out)中
                out.write(buffer, 0, length);
            }
            out.flush();
            out.close();
        }
    }
    
    @Override
    public Result getServiceRoleHostMapping(Integer clusterId) {
        return null;
    }
    
    @Override
    public Result checkServiceDependency(Integer clusterId, String serviceIds) {
        //
        List<ClusterServiceInstanceEntity> serviceInstanceList =
                serviceInstanceService.listRunningServiceInstance(clusterId);
        Map<String, ClusterServiceInstanceEntity> instanceMap =
                serviceInstanceList.stream()
                        .collect(
                                Collectors.toMap(
                                        ClusterServiceInstanceEntity::getServiceName,
                                        e -> e,
                                        (v1, v2) -> v1));
        
        List<FrameServiceEntity> list = frameService.listServices(serviceIds);
        Map<String, FrameServiceEntity> serviceMap =
                list.stream()
                        .collect(
                                Collectors.toMap(
                                        FrameServiceEntity::getServiceName,
                                        e -> e,
                                        (v1, v2) -> v1));
        if (!instanceMap.containsKey("ALERTMANAGER") && !serviceMap.containsKey("ALERTMANAGER")) {
            return Result.error(
                    "service install depends on alertmanager ,please make sure you have selected it or that alertmanager is normal and running");
        }
        if (!instanceMap.containsKey("GRAFANA") && !serviceMap.containsKey("GRAFANA")) {
            return Result.error(
                    "service install depends on grafana ,please make sure you have selected it or that grafana is normal and running");
        }
        if (!instanceMap.containsKey("PROMETHEUS") && !serviceMap.containsKey("PROMETHEUS")) {
            return Result.error(
                    "service install depends on prometheus ,please make sure you have selected it or that prometheus is normal and running");
        }
        
        for (FrameServiceEntity frameServiceEntity : list) {
            for (String dependService : frameServiceEntity.getDependencies().split(",")) {
                if (StringUtils.isNotBlank(dependService)
                        && !instanceMap.containsKey(dependService)
                        && !serviceMap.containsKey(dependService)) {
                    return Result.error(
                            ""
                                    + frameServiceEntity.getServiceName()
                                    + " install depends on "
                                    + dependService
                                    + ",please make sure that you have selected it or that "
                                    + dependService
                                    + " is normal and running");
                }
            }
        }
        return Result.success();
    }
    
    private ClusterServiceInstanceRoleGroup saveNewRoleGroup(
                                                             ClusterServiceInstanceEntity serviceInstanceEntity) {
        int count =
                roleGroupService.count(
                        new QueryWrapper<ClusterServiceInstanceRoleGroup>()
                                .eq(Constants.ROLE_GROUP_TYPE, "auto")
                                .eq(Constants.SERVICE_INSTANCE_ID, serviceInstanceEntity.getId()));
        ClusterServiceInstanceRoleGroup roleGroup = new ClusterServiceInstanceRoleGroup();
        int num = count + 1;
        roleGroup.setRoleGroupName("RoleGroup" + num);
        roleGroup.setServiceInstanceId(serviceInstanceEntity.getId());
        roleGroup.setServiceName(serviceInstanceEntity.getServiceName());
        roleGroup.setClusterId(serviceInstanceEntity.getClusterId());
        roleGroup.setRoleGroupType("auto");
        roleGroupService.save(roleGroup);
        return roleGroup;
    }
    
    private boolean isConfigNeedUpdate(
                                       ClusterServiceInstanceEntity serviceInstanceEntity, List<ServiceConfig> list) {
        List<ServiceConfig> originalConfigs =
                listServiceConfigByServiceInstance(serviceInstanceEntity);
        Map<String, Object> originalConfigMap =
                originalConfigs.stream()
                        .collect(
                                Collectors.toMap(
                                        ServiceConfig::getName,
                                        ServiceConfig::getValue,
                                        (v1, v2) -> v1));
        for (ServiceConfig serviceConfig : list) {
            String configName = serviceConfig.getName();
            String variableValue = String.valueOf(serviceConfig.getValue());
            if (originalConfigMap.containsKey(configName)) {
                String configValue = String.valueOf(originalConfigMap.get(configName));
                if (!variableValue.equals(configValue)) {
                    return true;
                }
            } else {
                return true;
            }
        }
        return false;
    }
    
    private void saveServiceRoleGroupConfig(
                                            Integer clusterId,
                                            String serviceName,
                                            List<ServiceConfig> list,
                                            HashMap<Generators, List<ServiceConfig>> configFileMap,
                                            ClusterServiceInstanceRoleGroup clusterServiceInstanceRoleGroup) {
        ClusterServiceRoleGroupConfig roleGroupConfig = new ClusterServiceRoleGroupConfig();
        roleGroupConfig.setRoleGroupId(clusterServiceInstanceRoleGroup.getId());
        roleGroupConfig.setClusterId(clusterId);
        roleGroupConfig.setCreateTime(new Date());
        roleGroupConfig.setUpdateTime(new Date());
        roleGroupConfig.setServiceName(serviceName);
        buildConfig(list, configFileMap, roleGroupConfig);
        roleGroupConfig.setConfigVersion(1);
        groupConfigService.save(roleGroupConfig);
    }
    
    private ClusterServiceInstanceRoleGroup saveServiceInstanceRoleGroup(
                                                                         Integer clusterId,
                                                                         String serviceName,
                                                                         ClusterServiceInstanceEntity serviceInstanceEntity) {
        ClusterServiceInstanceRoleGroup clusterServiceInstanceRoleGroup =
                new ClusterServiceInstanceRoleGroup();
        clusterServiceInstanceRoleGroup.setServiceInstanceId(serviceInstanceEntity.getId());
        clusterServiceInstanceRoleGroup.setClusterId(clusterId);
        clusterServiceInstanceRoleGroup.setRoleGroupName("默认角色组");
        clusterServiceInstanceRoleGroup.setServiceName(serviceName);
        clusterServiceInstanceRoleGroup.setRoleGroupType("default");
        roleGroupService.save(clusterServiceInstanceRoleGroup);
        return clusterServiceInstanceRoleGroup;
    }
    
    private ClusterServiceInstanceEntity saveServiceInstance(
                                                             Integer clusterId, String serviceName,
                                                             FrameServiceEntity frameServiceEntity) {
        ClusterServiceInstanceEntity serviceInstanceEntity;
        serviceInstanceEntity = new ClusterServiceInstanceEntity();
        serviceInstanceEntity.setClusterId(clusterId);
        serviceInstanceEntity.setServiceState(ServiceState.WAIT_INSTALL);
        serviceInstanceEntity.setServiceName(serviceName);
        serviceInstanceEntity.setLabel(frameServiceEntity.getLabel());
        serviceInstanceEntity.setCreateTime(new Date());
        serviceInstanceEntity.setUpdateTime(new Date());
        serviceInstanceEntity.setNeedRestart(NeedRestart.NO);
        serviceInstanceEntity.setFrameServiceId(frameServiceEntity.getId());
        serviceInstanceEntity.setSortNum(frameServiceEntity.getSortNum());
        serviceInstanceService.save(serviceInstanceEntity);
        return serviceInstanceEntity;
    }
    
    private void addHostNodeToPrometheus(
                                         Integer clusterId, HashMap<Generators, List<ServiceConfig>> configFileMap) {
        List<ClusterHostDO> hostList =
                hostService.list(
                        new QueryWrapper<ClusterHostDO>()
                                .eq(Constants.MANAGED, 1)
                                .eq(Constants.CLUSTER_ID, clusterId));
        Generators workerGenerators = new Generators();
        workerGenerators.setFilename("worker.json");
        workerGenerators.setOutputDirectory("configs");
        workerGenerators.setConfigFormat("custom");
        workerGenerators.setTemplateName("scrape.ftl");
        
        Generators nodeGenerators = new Generators();
        nodeGenerators.setFilename("linux.json");
        nodeGenerators.setOutputDirectory("configs");
        nodeGenerators.setConfigFormat("custom");
        nodeGenerators.setTemplateName("scrape.ftl");
        ArrayList<ServiceConfig> workerServiceConfigs = new ArrayList<>();
        ArrayList<ServiceConfig> nodeServiceConfigs = new ArrayList<>();
        for (ClusterHostDO clusterHostDO : hostList) {
            ServiceConfig serviceConfig = new ServiceConfig();
            serviceConfig.setName("worker_" + clusterHostDO.getHostname());
            serviceConfig.setValue(clusterHostDO.getHostname() + ":8585");
            serviceConfig.setRequired(true);
            workerServiceConfigs.add(serviceConfig);
            
            ServiceConfig nodeServiceConfig = new ServiceConfig();
            nodeServiceConfig.setName("node_" + clusterHostDO.getHostname());
            nodeServiceConfig.setValue(clusterHostDO.getHostname() + ":9100");
            nodeServiceConfig.setRequired(true);
            nodeServiceConfigs.add(nodeServiceConfig);
        }
        configFileMap.put(workerGenerators, workerServiceConfigs);
        configFileMap.put(nodeGenerators, nodeServiceConfigs);
    }
    
    private void buildConfigFileMap(
                                    String serviceName,
                                    ClusterInfoEntity clusterInfo,
                                    HashMap<String, ServiceConfig> map,
                                    HashMap<Generators, List<ServiceConfig>> configFileMap) {
        FrameServiceEntity frameService =
                this.frameService.getServiceByFrameCodeAndServiceName(
                        clusterInfo.getClusterFrame(), serviceName);
        if (StringUtils.isNotBlank(frameService.getConfigFileJson())) {
            Map<JSONObject, JSONArray> configMap =
                    JSONObject.parseObject(frameService.getConfigFileJson(), Map.class);
            for (JSONObject fileJson : configMap.keySet()) {
                Generators generators = fileJson.toJavaObject(Generators.class);
                List<ServiceConfig> serviceConfigs =
                        configMap.get(fileJson).toJavaList(ServiceConfig.class);
                for (ServiceConfig config : serviceConfigs) {
                    logger.info(config.getName());
                    if (map.containsKey(config.getName())) {
                        ServiceConfig newConfig = map.get(config.getName());
                        config.setValue(map.get(config.getName()).getValue());
                        config.setHidden(newConfig.isHidden());
                        config.setRequired(newConfig.isRequired());
                    }
                }
                configFileMap.put(generators, serviceConfigs);
            }
        }
    }
    
    private void addToGlobalVariable(Integer clusterId, String serviceName, String variableName, String value) {
        ClusterVariable clusterVariable =
                variableService.getVariableByVariableName(variableName, clusterId);
        if (Objects.nonNull(clusterVariable)) {
            if (!value.equals(clusterVariable.getVariableValue())) {
                clusterVariable.setServiceName(serviceName);
                clusterVariable.setVariableValue(value);
                variableService.updateById(clusterVariable);
            }
        } else {
            clusterVariable = new ClusterVariable();
            clusterVariable.setClusterId(clusterId);
            clusterVariable.setServiceName(serviceName);
            clusterVariable.setVariableName(variableName);
            clusterVariable.setVariableValue(value);
            variableService.save(clusterVariable);
        }
    }
    
    private void buildConfig(
                             List<ServiceConfig> list,
                             HashMap<Generators, List<ServiceConfig>> configFileMap,
                             ClusterServiceRoleGroupConfig roleGroupConfig) {
        String configJson = JSONObject.toJSONString(list);
        String configFileJson = JSONObject.toJSONString(configFileMap);
        roleGroupConfig.setConfigJson(configJson);
        roleGroupConfig.setConfigJsonMd5(SecureUtil.md5(configJson));
        roleGroupConfig.setConfigFileJson(configFileJson);
        roleGroupConfig.setConfigFileJsonMd5(SecureUtil.md5(configFileJson));
    }
    
    private void checkOnSameNode(Integer clusterId, List<ServiceRoleHostMapping> list) {
        Set<String> hostnameSet =
                list.stream()
                        .filter(s -> MUST_AT_SAME_NODE_BASIC_SERVICE.contains(s.getServiceRole()))
                        .map(ServiceRoleHostMapping::getHosts)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        if (CollectionUtils.isEmpty(hostnameSet)) {
            return;
        }
        
        Set<String> installedHostnameSet =
                roleInstanceService.lambdaQuery()
                        .eq(ClusterServiceRoleInstanceEntity::getClusterId, clusterId)
                        .in(
                                ClusterServiceRoleInstanceEntity::getServiceName,
                                MUST_AT_SAME_NODE_BASIC_SERVICE)
                        .list().stream()
                        .map(ClusterServiceRoleInstanceEntity::getHostname)
                        .collect(Collectors.toSet());
        hostnameSet.addAll(installedHostnameSet);
        
        if (hostnameSet.size() > 1) {
            throw new ServiceException(Status.BASIC_SERVICE_SELECT_MOST_ONE_HOST.getMsg());
        }
    }
    
    private void serviceValidation(ServiceRoleHostMapping serviceRoleHostMapping) {
        String serviceRole = serviceRoleHostMapping.getServiceRole();
        List<String> hosts = serviceRoleHostMapping.getHosts();
        
        if ("JournalNode".equals(serviceRole) && hosts.size() != 3) {
            throw new ServiceException(Status.THREE_JOURNALNODE_DEPLOYMENTS_REQUIRED.getMsg());
        }
        if ("NameNode".equals(serviceRole) && hosts.size() != 2) {
            throw new ServiceException(Status.TWO_NAMENODES_NEED_TO_BE_DEPLOYED.getMsg());
        }
        if ("ZKFC".equals(serviceRole) && hosts.size() != 2) {
            throw new ServiceException(Status.TWO_ZKFC_DEVICES_ARE_REQUIRED.getMsg());
        }
        if ("ResourceManager".equals(serviceRole) && hosts.size() != 2) {
            throw new ServiceException(Status.TWO_RESOURCEMANAGER_ARE_DEPLOYED.getMsg());
        }
        if ("ZkServer".equals(serviceRole) && (hosts.size() & 1) == 0) {
            throw new ServiceException(Status.ODD_NUMBER_ARE_REQUIRED_FOR_ZKSERVER.getMsg());
        }
        if ("DorisFE".equals(serviceRole) && (hosts.size() & 1) == 0) {
            throw new ServiceException(Status.ODD_NUMBER_ARE_REQUIRED_FOR_DORISFE.getMsg());
        }
        if ("KyuubiServer".equals(serviceRole) && hosts.size() != 2) {
            throw new ServiceException(Status.TWO_KYUUBISERVERS_NEED_TO_BE_DEPLOYED.getMsg());
        }
    }
    
    private List<ServiceConfig> listServiceConfigByServiceInstance(
                                                                   ClusterServiceInstanceEntity serviceInstance) {
        ClusterServiceInstanceRoleGroup roleGroup =
                roleGroupService.getRoleGroupByServiceInstanceId(serviceInstance.getId());
        ClusterServiceRoleGroupConfig config =
                groupConfigService.getConfigByRoleGroupId(roleGroup.getId());
        return JSONArray.parseArray(config.getConfigJson(), ServiceConfig.class);
    }
}
