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

package com.datasophon.worker.strategy;

import akka.actor.ActorRef;
import cn.hutool.core.net.NetUtil;
import com.datasophon.common.command.OlapOpsType;
import com.datasophon.common.command.OlapSqlExecCommand;
import com.datasophon.common.command.ServiceRoleOperateCommand;
import com.datasophon.common.enums.CommandType;
import com.datasophon.common.utils.ExecResult;
import com.datasophon.common.utils.ThrowableUtils;
import com.datasophon.worker.handler.ServiceHandler;
import com.datasophon.worker.utils.ActorUtils;

public class BEHandlerStrategy extends AbstractHandlerStrategy implements ServiceRoleStrategy {
    
    public BEHandlerStrategy(String serviceName, String serviceRoleName) {
        super(serviceName, serviceRoleName);
    }
    
    @Override
    public ExecResult handler(ServiceRoleOperateCommand command) {
        ExecResult startResult = new ExecResult();
        ServiceHandler serviceHandler = new ServiceHandler(command.getServiceName(), command.getServiceRoleName());
        
        if (command.getCommandType().equals(CommandType.INSTALL_SERVICE)) {
            logger.info("prepare to add be to cluster before start");
            try {
                
                OlapSqlExecCommand sqlExecCommand = new OlapSqlExecCommand();
                sqlExecCommand.setFeMaster(command.getMasterHost());
                sqlExecCommand.setHostName(NetUtil.getLocalhostStr());
                sqlExecCommand.setOpsType(OlapOpsType.ADD_BE);
                
                ActorUtils.getRemoteActor(command.getManagerHost(), "masterNodeProcessingActor")
                        .tell(sqlExecCommand, ActorRef.noSender());
                
                logger.info("add be command sent to master successfully");
                
            } catch (Exception e) {
                logger.error("add backend failed: {}", ThrowableUtils.getStackTrace(e));
                startResult.setExecResult(false);
                startResult.setExecOut("Failed to add backend to FE before start.");
                return startResult;
            }
            
            logger.info("starting slave be...");
            startResult = serviceHandler.start(command.getStartRunner(), command.getStatusRunner(),
                    command.getDecompressPackageName(), command.getRunAs());
            
            if (startResult.getExecResult()) {
                logger.info("slave be start success");
            } else {
                logger.error("slave be start failed");
            }
            
        } else {
            // 非安装类命令，直接启动
            startResult = serviceHandler.start(command.getStartRunner(), command.getStatusRunner(),
                    command.getDecompressPackageName(), command.getRunAs());
        }
        
        return startResult;
    }
    
}
