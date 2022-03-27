/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package model

import (
	"math/rand"
)

const (
	Static DiscoveryType = 0 + iota
	StrictDNS
	LogicalDns
	EDS
	OriginalDst
)

var (
	// DiscoveryTypeName
	DiscoveryTypeName = map[int32]string{
		0: "Static",
		1: "StrictDNS",
		2: "LogicalDns",
		3: "EDS",
		4: "OriginalDst",
	}

	// DiscoveryTypeValue
	DiscoveryTypeValue = map[string]int32{
		"Static":      0,
		"StrictDNS":   1,
		"LogicalDns":  2,
		"EDS":         3,
		"OriginalDst": 4,
	}
)

type (
	// Cluster a single upstream cluster
	Cluster struct {
		Name                 string           `yaml:"name" json:"name"` // Name the cluster unique name
		TypeStr              string           `yaml:"type" json:"type"` // Type the cluster discovery type string value
		Type                 DiscoveryType    `yaml:"-" json:"-"`       // Type the cluster discovery type
		EdsClusterConfig     EdsClusterConfig `yaml:"eds_cluster_config" json:"eds_cluster_config" mapstructure:"eds_cluster_config"`
		LbStr                string           `yaml:"lb_policy" json:"lb_policy"`   // Lb the cluster select node used loadBalance policy
		Lb                   LbPolicy         `yaml:",omitempty" json:",omitempty"` // Lb the cluster select node used loadBalance policy
		HealthChecks         []HealthCheck    `yaml:"health_checks" json:"health_checks"`
		Endpoints            []*Endpoint      `yaml:"endpoints" json:"endpoints"`
		prePickEndpointIndex int
	}

	// EdsClusterConfig
	EdsClusterConfig struct {
		EdsConfig   ConfigSource `yaml:"eds_config" json:"eds_config" mapstructure:"eds_config"`
		ServiceName string       `yaml:"service_name" json:"service_name" mapstructure:"service_name"`
	}

	// Registry remote registry where dubbo apis are registered.
	Registry struct {
		Protocol string `default:"zookeeper" yaml:"protocol" json:"protocol"`
		Timeout  string `yaml:"timeout" json:"timeout"`
		Address  string `yaml:"address" json:"address"`
		Username string `yaml:"username" json:"username"`
		Password string `yaml:"password" json:"password"`
	}

	// DiscoveryType
	DiscoveryType int32

	// Endpoint 一个服务实例
	Endpoint struct {
		ID      string        `yaml:"ID" json:"ID"`     // ID indicate one endpoint
		Name    string        `yaml:"name" json:"name"` // Name the cluster unique name
		Address SocketAddress `yaml:"socket_address" json:"socket_address" mapstructure:"socket_address"`
		// extra info such as label or other meta data
		Metadata map[string]string `yaml:"meta" json:"meta"`
		Healthy  bool              `yaml:"healthy" json:"healthy"`
	}

	/**
	 * 第一版先负责实现健康度累加的情况
	 * 后续根据每台机器的服务实例为维度进行分组统计
	 */
	//Healthy struct {
	//	Healthy string   `yaml:"Healthy" json:"Healthy"` //provider服务实例的健康状态
	//	HealthWeight int `yaml:"HealthWeight" json:"HealthWeight"` //在健康检查当中 心跳线程每次上报 健康度+1 需要配合PickEndpoint的负载均衡权重计算
	//	consumerIsRemove bool `yaml:"consumerIsRemove" json:"consumerIsRemove"`  //当consumer的健康度为负数的时候，需要摘除，即标记下线 跑出异常让用户重新注册
	//	providerIsRemove bool  `yaml:"providerIsRemove" json:"providerIsRemove"`
	//
	//}
)

/**
 * 需要设置到注册中心的节点下 作为一个属性
 */
func (c *Cluster) ChangeHealth(healthWeight bool, ipAddress string) *bool {
	return nil
}

func (c *Cluster) PickOneEndpoint() *Endpoint {
	// TODO: add lb strategy abstraction
	if c.Endpoints == nil || len(c.Endpoints) == 0 {
		return nil
	}

	if len(c.Endpoints) == 1 {
		return c.Endpoints[0]
	}

	if c.Lb == Rand {
		return c.Endpoints[rand.Intn(len(c.Endpoints))]
	} else if c.Lb == RoundRobin {

		//FIXME 假设现在有一批服务实例列表 采用轮训负载
		// prePickEndpointIndex 代表每一次pixiu gateway发起的请求
		// 负载到编号哪几台机器上
		lens := len(c.Endpoints)
		if c.prePickEndpointIndex >= lens {
			c.prePickEndpointIndex = 0
		}
		e := c.Endpoints[c.prePickEndpointIndex]
		c.prePickEndpointIndex = (c.prePickEndpointIndex + 1) % lens
		return e
	} else {
		return c.Endpoints[rand.Intn(len(c.Endpoints))]
	}
}
