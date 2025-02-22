package zookeeper

import (
	"github.com/go-zookeeper/zk"
	"mosn.io/layotto/components/lock"
	"mosn.io/layotto/components/pkg/utils"
	"mosn.io/pkg/log"
	util "mosn.io/pkg/utils"
	"time"
)

type ZookeeperLock struct {
	//trylock reestablish connection  every time
	factory utils.ConnectionFactory
	//unlock reuse this conneciton
	unlockConn utils.ZKConnection
	metadata   utils.ZookeeperMetadata
	logger     log.ErrorLogger
}

func NewZookeeperLock(logger log.ErrorLogger) *ZookeeperLock {
	lock := &ZookeeperLock{
		logger: logger,
	}
	return lock
}

func (p *ZookeeperLock) Init(metadata lock.Metadata) error {

	m, err := utils.ParseZookeeperMetadata(metadata.Properties)
	if err != nil {
		return err
	}

	p.metadata = m
	p.factory = &utils.ConnectionFactoryImpl{}

	//init unlock connection
	zkConn, err := p.factory.NewConnection(p.metadata.SessionTimeout, p.metadata)
	if err != nil {
		return err
	}
	p.unlockConn = zkConn
	return nil
}

func (p *ZookeeperLock) Features() []lock.Feature {
	return nil
}
func (p *ZookeeperLock) TryLock(req *lock.TryLockRequest) (*lock.TryLockResponse, error) {

	conn, err := p.factory.NewConnection(time.Duration(req.Expire)*time.Second, p.metadata)
	if err != nil {
		return &lock.TryLockResponse{}, err
	}
	//1.create zk ephemeral node
	_, err = conn.Create("/"+req.ResourceId, []byte(req.LockOwner), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))

	//2.1 create node fail ,indicates lock fail
	if err != nil {
		defer conn.Close()
		//the node exists,lock fail
		if err == zk.ErrNodeExists {
			return &lock.TryLockResponse{
				Success: false,
			}, nil
		}
		//other err
		return nil, err
	}

	//2.2 create node success, asyn  to make sure zkclient alive for need time
	util.GoWithRecover(func() {
		//can also
		//time.Sleep(time.Second * time.Duration(req.Expire))
		timeAfterTrigger := time.After(time.Second * time.Duration(req.Expire))
		<-timeAfterTrigger
		// make sure close connecion
		conn.Close()
	}, nil)

	return &lock.TryLockResponse{
		Success: true,
	}, nil

}
func (p *ZookeeperLock) Unlock(req *lock.UnlockRequest) (*lock.UnlockResponse, error) {

	conn := p.unlockConn

	path := "/" + req.ResourceId
	owner, state, err := conn.Get(path)

	if err != nil {
		//node does not exist, indicates this lock has expired
		if err == zk.ErrNoNode {
			return &lock.UnlockResponse{Status: lock.LOCK_UNEXIST}, nil
		}
		//other err
		return nil, err
	}
	//node exist ,but owner not this, indicates this lock has occupied or wrong unlock
	if string(owner) != req.LockOwner {
		return &lock.UnlockResponse{Status: lock.LOCK_BELONG_TO_OTHERS}, nil
	}
	err = conn.Delete(path, state.Version)
	//owner is this, but delete fail
	if err != nil {
		// delete no node , indicates this lock has expired
		if err == zk.ErrNoNode {
			return &lock.UnlockResponse{Status: lock.LOCK_UNEXIST}, nil
			// delete version error , indicates this lock has occupied by others
		} else if err == zk.ErrBadVersion {
			return &lock.UnlockResponse{Status: lock.LOCK_BELONG_TO_OTHERS}, nil
			//other error
		} else {
			return nil, err
		}
	}
	//delete success, unlock success
	return &lock.UnlockResponse{Status: lock.SUCCESS}, nil
}
