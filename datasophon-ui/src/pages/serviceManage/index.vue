<!--
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


 * @Date: 2022-05-24 10:28:22
 * @LastEditTime: 2022-09-26 17:15:22
 * @FilePath: \ddh-ui\src\pages\serviceManage\index.vue
-->
<template>
  <div class="service-list card-shadow">
    <a-tabs v-model="tabKey" @change="callback">
      <a-tab-pane :key="1" tab="总览" v-if="pageOverview">
        <OverViewPage :serviceId="serviceId" />
      </a-tab-pane>
      <a-tab-pane :key="2" tab="实例">
        <ExampleList ref="ExampleListRef" :serviceId="serviceId" :tabKey="tabKey"/>
      </a-tab-pane>
      <a-tab-pane :key="3" tab="配置">
        <Setting :tabKey="tabKey"/>
      </a-tab-pane>
      <a-tab-pane v-if="serviceName === 'YARN'" :key="4" tab="资源配置">
        <Queue />
      </a-tab-pane>
    </a-tabs>
    <a-dropdown class="webui" :style="{left: serviceName === 'YARN' ? '280px' : '200px'}" v-if="webUis.length > 0">
      <a-menu slot="overlay" @click="handleMenuClick">
        <a-menu-item v-for="(item, index) in webUis" :key="index">{{item.name}}</a-menu-item>
      </a-menu>
      <div class="mgr12">
        WebUI
        <a-icon type="down" />
      </div>
    </a-dropdown>
    <div v-else class="webui" :style="{left: getWebUIWidth(serviceName)}">
      WebUI
      <a-icon type="down" />
    </div>
  </div>
</template>

<script>
import ExampleList from "./exampleList.vue";
// import OverViewPage from "./overViewPage.vue";
const OverViewPage = () => import ('./overViewPage.vue')
import Setting from "./setting.vue";
import Queue from './queue.vue'
export default {
  name: "ServiceList",
  components: { ExampleList, Setting, OverViewPage, Queue },
  data() {
    return {
      tabKey: 1,
      serviceName: '',
      loading: false,
      tabList: ["总览", "实例", "配置"],
      serviceId: "",
      webUis: [],
      pageOverview: true,
      tableColumns: [
        { title: "序号", key: "index" },
        { title: "角色类型", key: "serviceName" },
        { title: "主机", key: "serviceVersion" },
        { title: "状态", key: "serviceDesc" },
        { title: "操作", key: "action" },
      ],
    };
  },
  watch: {
    $route: function (val, oldVal) {
      if (this.$store.state.setting.serviceId === val.params.serviceId) return false
      this.$store.commit('setting/setServiceId', val.params.serviceId)
      this.serviceId = val.params.serviceId;
    }
  },
  methods: {
    getWebUIWidth (serviceName) {
      if (serviceName === 'KRBCLIENT') {
        return '136px'
      } else {
        return serviceName === 'YARN' ? '280px' : '200px'
      }
    },
    handleMenuClick(item) {
      let url = this.webUis[item.key].webUrl
      window.open(url)
    },
    callback(key) {
      console.log(key);
      this.tabKey = key;
    },
    getWebUis() {
      this.$axiosPost(global.API.getWebUis, {
        serviceInstanceId: this.$route.params.serviceId,
      }).then((res) => {
        if (res.code === 200) {
          this.webUis = res.data || [];
        }
      });
    },
    getServiceName () {
      if (this.$route && this.$route.params && this.$route.params.serviceId) {
        let name = ''
        const serviceId = this.$route.params.serviceId || ''
        const menuData = JSON.parse(localStorage.getItem('menuData')) || []
        const arr = menuData.filter(item => item.path === 'service-manage')
        if (arr.length > 0) {
          arr[0].children.map(item => {
            if (item.meta.params.serviceId == serviceId) {
              name = item.name
              // console.log("grafana url: " + item.meta.obj.dashboardUrl)
              this.pageOverview = (item.meta.obj.dashboardUrl != undefined && item.meta.obj.dashboardUrl != "")
              this.tabKey = (this.pageOverview ? 1 : 2); // 如果没有总览，则显示实例为第二个页签
            }
          })
          this.serviceName = name
        }
      }
    }
  },
  mounted() {
    this.getWebUis();
    this.getServiceName()
  },
  activated () {
    console.log('每次我只触发一次')
    this.serviceId = this.$route.params.serviceId;
    this.getWebUis();
    this.getServiceName()
    console.log(this.$route, 'sdadadasd')
  },
  deactivated () {
    console.log('每次我buxiang只触发一次')
  }
};
</script>

<style lang="less" scoped>
.service-list {
  background: #fff;
  padding: 0 20px 20px;
  position: relative;
  .webui {
    position: absolute;
    left: 200px;
    top: 12px;
  }
}
</style>
