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

package com.datasophon.api.master;

import com.datasophon.api.master.alert.ServiceRoleCheckActor;
import com.datasophon.common.command.ClusterCommand;
import com.datasophon.common.command.HostCheckCommand;
import com.datasophon.common.command.ServiceRoleCheckCommand;
import com.datasophon.common.enums.ClusterCommandType;

import org.apache.commons.lang3.StringUtils;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.util.Timeout;

public class ActorUtils {
    
    private static final Logger logger = LoggerFactory.getLogger(ActorUtils.class);
    
    public static ActorSystem actorSystem;
    
    public static final String DATASOPHON = "datasophon";
    
    public static final String AKKA_REMOTE_NETTY_TCP_HOSTNAME = "akka.remote.netty.tcp.hostname";
    
    private static Random rand;
    
    private ActorUtils() throws NoSuchAlgorithmException {
    }
    
    public static void init() throws UnknownHostException, NoSuchAlgorithmException {
        String hostname = InetAddress.getLocalHost().getHostName();
        Config config = ConfigFactory.parseString(AKKA_REMOTE_NETTY_TCP_HOSTNAME + "=" + hostname);
        actorSystem = ActorSystem.create(DATASOPHON, config.withFallback(ConfigFactory.load()));
        actorSystem.actorOf(Props.create(WorkerStartActor.class), getActorRefName(WorkerStartActor.class));
        ActorRef serviceRoleCheckActor = actorSystem.actorOf(Props.create(ServiceRoleCheckActor.class),
                getActorRefName(ServiceRoleCheckActor.class));
        ActorRef hostCheckActor =
                actorSystem.actorOf(Props.create(HostCheckActor.class), getActorRefName(HostCheckActor.class));
        actorSystem.actorOf(Props.create(MasterNodeProcessingActor.class),
                getActorRefName(MasterNodeProcessingActor.class));
        
        ActorRef clusterCheckActor =
                actorSystem.actorOf(Props.create(ClusterActor.class), getActorRefName(ClusterActor.class));
        
        // 节点检测 5m 检测一次
        actorSystem.scheduler().schedule(
                FiniteDuration.apply(30L, TimeUnit.SECONDS),
                FiniteDuration.apply(300L, TimeUnit.SECONDS),
                hostCheckActor,
                new HostCheckCommand(),
                actorSystem.dispatcher(),
                ActorRef.noSender());

        // 角色检测 15s 检测一次
        actorSystem.scheduler().schedule(
                FiniteDuration.apply(15L, TimeUnit.SECONDS),
                FiniteDuration.apply(15L, TimeUnit.SECONDS),
                serviceRoleCheckActor,
                new ServiceRoleCheckCommand(),
                actorSystem.dispatcher(),
                ActorRef.noSender());
        
        // 集群检测 1m 检测一次
        actorSystem.scheduler().schedule(
                FiniteDuration.apply(30L, TimeUnit.SECONDS),
                FiniteDuration.apply(60L, TimeUnit.SECONDS),
                clusterCheckActor,
                new ClusterCommand(ClusterCommandType.CHECK),
                actorSystem.dispatcher(),
                ActorRef.noSender());
        
        rand = SecureRandom.getInstanceStrong();
    }
    
    public static ActorRef getLocalActor(Class actorClass, String actorName) {
        ActorSelection actorSelection = actorSystem.actorSelection("/user/" + actorName);
        Timeout timeout = new Timeout(Duration.create(30, TimeUnit.SECONDS));
        Future<ActorRef> future = actorSelection.resolveOne(timeout);
        ActorRef actorRef = null;
        try {
            actorRef = Await.result(future, Duration.create(30, TimeUnit.SECONDS));
        } catch (Exception e) {
            logger.warn("{} actor not found , create it", actorName);
        }
        if (Objects.isNull(actorRef)) {
            logger.info("create actor {}", actorName);
            actorRef = createActor(actorClass, actorName);
        } else {
            logger.info("find actor {}", actorName);
        }
        return actorRef;
    }
    
    private static ActorRef createActor(Class actorClass, String actorName) {
        ActorRef actorRef;
        try {
            actorRef =
                    actorSystem.actorOf(Props.create(actorClass).withDispatcher("my-forkjoin-dispatcher"), actorName);
        } catch (Exception e) {
            int num = rand.nextInt(1000);
            actorRef = actorSystem.actorOf(Props.create(actorClass).withDispatcher("my-forkjoin-dispatcher"),
                    actorName + num);
        }
        
        return actorRef;
    }
    
    public static ActorRef getRemoteActor(String hostname, String actorName) {
        String actorPath = "akka.tcp://datasophon@" + hostname + ":2552/user/worker/" + actorName;
        ActorSelection actorSelection = actorSystem.actorSelection(actorPath);
        Timeout timeout = new Timeout(Duration.create(30, TimeUnit.SECONDS));
        Future<ActorRef> future = actorSelection.resolveOne(timeout);
        ActorRef actorRef = null;
        try {
            actorRef = Await.result(future, Duration.create(30, TimeUnit.SECONDS));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        
        return actorRef;
    }
    
    /**
     * shutdown
     */
    public static void shutdown() {
        if (actorSystem != null) {
            try {
                actorSystem.shutdown();
            } catch (Exception ignore) {
            }
            actorSystem = null;
        }
    }
    
    /**
     * Get ActorRef name from Class name.
     */
    public static String getActorRefName(Class clazz) {
        return StringUtils.uncapitalize(clazz.getSimpleName());
    }
    
}
