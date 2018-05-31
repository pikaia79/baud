package topo

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util/log"
	"path"
	"time"
)

func (s *TopoServer) AddTask(ctx context.Context, zoneName string, task *metapb.Task, timeout time.Duration) error {
	if ctx == nil || task == nil {
		return ErrNoNode
	}

	contents, err := proto.Marshal(task)
	if err != nil {
		log.Error("Fail to marshal meta data for task[%v]. err[%v]", task, err)
		return err
	}

	nodePath := path.Join(tasksPath, task.Type, membersPath, task.Id, TaskTopoFile)
	_, err = s.backend.CreateUniqueEphemeral(ctx, zoneName, nodePath, contents, timeout)
	return err
}

func (s *TopoServer) GetTask(ctx context.Context, zoneName string, taskType string, taskId string) (*metapb.Task, error) {
	if ctx == nil || len(zoneName) == 0 || len(taskType) == 0 || len(taskId) == 0 {
		return nil, ErrNoNode
	}

	nodePath := path.Join(tasksPath, taskType, membersPath, taskId, TaskTopoFile)
	contents, _, err := s.backend.Get(ctx, zoneName, nodePath)
	if err != nil {
		return nil, err
	}

	task := &metapb.Task{}
	if err := proto.Unmarshal(contents, task); err != nil {
		log.Error("Fail to unmarshal meta data for task[%s][%s]. err[%v]", taskType, taskId, err)
		return nil, err
	}

	return task, nil
}
