package test

import (
	"context"
	"testing"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/common/logger"
	"dubbo.apache.org/dubbo-go/v3/config"
	"dubbo.apache.org/dubbo-go/v3/config/generic"
	_ "dubbo.apache.org/dubbo-go/v3/imports"

	hessian "github.com/apache/dubbo-go-hessian2"

	tpconst "github.com/dubbogo/triple/pkg/common/constant"
)

func TestTriple2Dubbo(t *testing.T) {
	tripleRefConf := newTripleRefConf("com.dubbogo.pixiu.DubboUserService", tpconst.TRIPLE)
	resp, err := tripleRefConf.GetRPCService().(*generic.GenericService).Invoke(
		context.TODO(),
		"GetUserByName",
		[]string{"java.lang.String"},
		[]hessian.Object{"tc"},
	)

	if err != nil {
		panic(err)
	}
	logger.Infof("GetUser1(userId string) res: %+v", resp)
}

func newTripleRefConf(iface, protocol string) config.ReferenceConfig {

	refConf := config.ReferenceConfig{
		InterfaceName: iface,
		Cluster:       "failover",
		RegistryIDs:   []string{"zk"},
		Protocol:      protocol,
		Generic:       "true",
		Group:         "test",
		Version:       "1.0.0",
		URL:           "tri://127.0.0.1:9999/" + iface + "?" + constant.SerializationKey + "=hessian2",
	}

	rootConfig := config.NewRootConfigBuilder().
		Build()
	if err := config.Load(config.WithRootConfig(rootConfig)); err != nil {
		panic(err)
	}
	_ = refConf.Init(rootConfig)
	refConf.GenericLoad(appName)

	return refConf
}
