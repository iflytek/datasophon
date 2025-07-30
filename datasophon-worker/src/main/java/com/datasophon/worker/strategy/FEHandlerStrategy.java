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
import cn.hutool.json.JSONUtil;
import com.datasophon.common.command.OlapOpsType;
import com.datasophon.common.command.OlapSqlExecCommand;
import com.datasophon.common.command.ServiceRoleOperateCommand;
import com.datasophon.common.enums.CommandType;
import com.datasophon.common.model.ServiceRoleRunner;
import com.datasophon.common.utils.ExecResult;
import com.datasophon.common.utils.ThrowableUtils;
import com.datasophon.worker.handler.ServiceHandler;
import com.datasophon.worker.utils.ActorUtils;

import java.util.ArrayList;

public class FEHandlerStrategy extends AbstractHandlerStrategy implements ServiceRoleStrategy {
    
    public FEHandlerStrategy(String serviceName, String serviceRoleName) {
        super(serviceName, serviceRoleName);
    }
    
    @Override
    public ExecResult handler(ServiceRoleOperateCommand command) {
        ExecResult startResult = new ExecResult();
        logger.info("FEHandlerStrategy start fe: {}", JSONUtil.toJsonStr(command));
        ServiceHandler serviceHandler = new ServiceHandler(command.getServiceName(), command.getServiceRoleName());
        
        if (command.getCommandType() == CommandType.INSTALL_SERVICE) {
            if (command.isSlave()) {
                logger.info("prepare to add slave fe follower before start");
                
                try {
                    // 先尝试添加 follower
                    OlapSqlExecCommand sqlExecCommand = new OlapSqlExecCommand();
                    sqlExecCommand.setFeMaster(command.getMasterHost());
                    sqlExecCommand.setHostName(NetUtil.getLocalhostStr());
                    sqlExecCommand.setOpsType(OlapOpsType.ADD_FE_FOLLOWER);
                    
                    ActorUtils.getRemoteActor(command.getManagerHost(), "masterNodeProcessingActor")
                            .tell(sqlExecCommand, ActorRef.noSender());
                    
                    logger.info("slave fe follower add command sent to master");
                    
                } catch (Exception e) {
                    logger.error("add slave fe follower failed: {}", ThrowableUtils.getStackTrace(e));
                    startResult.setExecResult(false);
                    startResult.setExecOut("Failed to add follower before start.");
                    return startResult;
                }
                
                logger.info("starting slave fe...");
                ArrayList<String> commands = new ArrayList<>();
                commands.add("--helper");
                commands.add(command.getMasterHost() + ":9010");
                commands.add("--daemon");
                
                ServiceRoleRunner startRunner = new ServiceRoleRunner();
                startRunner.setProgram(command.getStartRunner().getProgram());
                startRunner.setArgs(commands);
                startRunner.setTimeout("600");
                
                startResult = serviceHandler.start(startRunner, command.getStatusRunner(),
                        command.getDecompressPackageName(), command.getRunAs());
                
                if (startResult.getExecResult()) {
                    logger.info("slave fe started successfully");
                } else {
                    logger.error("slave fe start failed");
                }
                
            } else {
                startResult = serviceHandler.start(command.getStartRunner(), command.getStatusRunner(),
                        command.getDecompressPackageName(), command.getRunAs());
            }
        } else {
            startResult = serviceHandler.start(command.getStartRunner(), command.getStatusRunner(),
                    command.getDecompressPackageName(), command.getRunAs());
        }
        
        return startResult;
    }
    
}
