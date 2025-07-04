package resolver

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"time"

	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/go-zookeeper/zk"
	"github.com/kitex-contrib/registry-zookeeper/entity"
	"github.com/kitex-contrib/registry-zookeeper/utils"
)

type zookeeperResolver struct {
	conn *zk.Conn
}

func NewZookeeperResolver(servers []string, sessionTimeout time.Duration) (discovery.Resolver, error) {
	conn, _, err := zk.Connect(servers, sessionTimeout)
	if err != nil {
		return nil, err
	}
	return &zookeeperResolver{
		conn: conn,
	}, nil
}
func NewZookeeperResolverWithAuth(servers []string, sessionTimeout time.Duration, user, passward string) (discovery.Resolver, error) {
	conn, _, err := zk.Connect(servers, sessionTimeout)
	if err != nil {
		return nil, err
	}
	if err := conn.AddAuth(utils.Scheme, []byte(user+":"+passward)); err != nil {
		return nil, err
	}
	return &zookeeperResolver{
		conn: conn,
	}, nil
}

//要实现discovery.Resolver接口
// // Resolver resolves the target endpoint into a list of Instance.
// type Resolver interface {
// 	// Target should return a description for the given target that is suitable for being a key for cache.
// 	Target(ctx context.Context, target rpcinfo.EndpointInfo) (description string)

// 	// Resolve returns a list of instances for the given description of a target.
// 	Resolve(ctx context.Context, desc string) (Result, error)

// 	// Diff computes the difference between two results.
// 	// When `next` is cacheable, the Change should be cacheable, too. And the `Result` field's CacheKey in
// 	// the return value should be set with the given cacheKey.
// 	Diff(cacheKey string, prev, next Result) (Change, bool)

//		// Name returns the name of the resolver.
//		Name() string
//	}
func (r *zookeeperResolver) Target(ctx context.Context, target rpcinfo.EndpointInfo) (description string) {
	return target.ServiceName()
}

// 解析服务名，查找服务实例
func (r *zookeeperResolver) Resolve(ctx context.Context, desc string) (discovery.Result, error) {
	path := desc
	if !strings.HasPrefix(path, utils.Separator) {
		//utils.Seperator="/"
		path = utils.Separator + path
	}
	childPoints, err := r.getChildPoints(path)
	if err != nil {
		return discovery.Result{}, err
	}
	instances, err := r.getInstances(path, childPoints)
	return discovery.Result{
		Cacheable: true,
		CacheKey:  path,
		Instances: instances,
	}, err
}
func (r *zookeeperResolver) Diff(cacheKey string, prev, next discovery.Result) (discovery.Change, bool) {
	return discovery.DefaultDiff(cacheKey, prev, next)
}
func (r *zookeeperResolver) Name() string {
	return "zookeeper"
}

// -------------------------------------------------
func (r *zookeeperResolver) getChildPoints(path string) ([]string, error) {
	children, _, err := r.conn.Children(path)
	return children, err
}
func (r *zookeeperResolver) getInstances(path string, childrenPoints []string) ([]discovery.Instance, error) {
	instances := make([]discovery.Instance, 0)
	for _, child := range childrenPoints {
		//为啥要判断host或者port?
		host, port, err := net.SplitHostPort(child)
		if err != nil {
			return []discovery.Instance{}, err
		}
		//要是host没有
		if host == "" {
			return []discovery.Instance{}, err
		}
		if port == "" {
			return []discovery.Instance{}, err
		}
		ins, err := r.getInstance(path, child)
		if err != nil {
			return nil, err
		}
		instances = append(instances, ins)
	}
	return instances, nil
}

// 获取子结点详细信息
func (r *zookeeperResolver) getInstance(path, child string) (discovery.Instance, error) {
	data, _, err := r.conn.Get(path + utils.Separator + child)
	if err != nil {
		return nil, err
	}
	//var entity utils.RegisteryEntity
	en := new(entity.RegistryEntity)
	if err := json.Unmarshal(data, en); err != nil {
		return nil, err
	}
	return discovery.NewInstance("tcp", child, en.Weight, en.Tags), nil
}
