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

package com.datasophon.api.master.alert;

import com.datasophon.api.service.ClusterServiceRoleInstanceService;
import com.datasophon.api.strategy.ServiceRoleStrategy;
import com.datasophon.api.strategy.ServiceRoleStrategyContext;
import com.datasophon.api.utils.SpringTool;
import com.datasophon.common.Constants;
import com.datasophon.common.enums.ServiceRoleType;
import com.datasophon.common.command.ServiceRoleCheckCommand;
import com.datasophon.dao.entity.ClusterServiceRoleInstanceEntity;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;

import akka.actor.UntypedActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceRoleCheckActor extends UntypedActor {

    private static final Logger logger = LoggerFactory.getLogger(ServiceRoleCheckActor.class);

    @Override
    public void onReceive(Object msg) throws Throwable {
        if (msg instanceof ServiceRoleCheckCommand) {
            logger.info("start to service role info");
            ClusterServiceRoleInstanceService roleInstanceService =
                    SpringTool.getApplicationContext()
                            .getBean(ClusterServiceRoleInstanceService.class);
            
            List<ClusterServiceRoleInstanceEntity> list =
                    roleInstanceService.list(
                            new QueryWrapper<ClusterServiceRoleInstanceEntity>()
                                    .ne(
                                            Constants.ROLE_TYPE,
                                            ServiceRoleType.CLIENT.getCode());
            
            if (!list.isEmpty()) {
                Map<String, ClusterServiceRoleInstanceEntity> map = translateListToMap(list);
                for (ClusterServiceRoleInstanceEntity roleInstanceEntity : list) {
                    ServiceRoleStrategy serviceRoleHandler =
                            ServiceRoleStrategyContext.getServiceRoleHandler(
                                    roleInstanceEntity.getServiceRoleName());
                    if (Objects.nonNull(serviceRoleHandler)) {
                        serviceRoleHandler.handlerServiceRoleCheck(roleInstanceEntity, map);
                    }
                }
            } else {
                unhandled(msg);
            }
        }
    }
    
    private Map<String, ClusterServiceRoleInstanceEntity> translateListToMap(
                                                                             List<ClusterServiceRoleInstanceEntity> list) {
        return list.stream()
                .collect(
                        Collectors.toMap(
                                e -> e.getHostname() + e.getServiceRoleName(),
                                e -> e,
                                (v1, v2) -> v1));
    }
}
