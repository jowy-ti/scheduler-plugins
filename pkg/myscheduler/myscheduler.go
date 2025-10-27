package myscheduler

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

type MyScheduler struct {
	handle framework.Handle
}

const Name = "MyScheduler"

// var err string
// var log string

func (m *MyScheduler) Name() string {
	return Name
}

func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &MyScheduler{handle: h}, nil
}

func (m *MyScheduler) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {

	// var pLabelExist bool = false
	var nLabelExist bool = false

	node := nodeInfo.Node()

	nodeName := node.Name
	podLabels := pod.Labels
	nodeLabels := node.Labels

	if nodeLabels == nil {
		return framework.NewStatus(framework.Unschedulable, "No labels in the Node")
	}

	// Filter fake pods and kwok nodes
	if podLabels != nil {
		podApp, pLabelExist := podLabels["app"]

		if pLabelExist && podApp == "fake-pod" {
			nodeType, nLabelExist := nodeLabels["type"]

			klog.V(4).Infof("%s type: %s ", nodeName, nodeType)

			if !nLabelExist || nodeType != "kwok" {
				err := fmt.Sprintf("node %s label 'type': %s ", nodeName, nodeType)
				return framework.NewStatus(framework.Unschedulable, err)
			}
		}
	}

	// Filter nodes by GPU

	nodeGPU, nLabelExist := nodeLabels["nvidia.com/gpu.present"]

	klog.V(4).Infof("%s nvidia.com/gpu.present: %s ", nodeName, nodeGPU)

	if !nLabelExist || nodeGPU != "true" {
		err := fmt.Sprintf("node %s label 'nvidia.com/gpu.present': %s ", nodeName, nodeGPU)
		return framework.NewStatus(framework.Unschedulable, err)
	}

	return framework.NewStatus(framework.Success)
}
