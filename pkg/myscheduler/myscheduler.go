package myscheduler

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	klog "k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

type MyScheduler struct {
	handle framework.Handle
}

const Name = "MyScheduler"

func (m *MyScheduler) Name() string {
	return Name
}

func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &MyScheduler{handle: h}, nil
}

func (m *MyScheduler) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	node := nodeInfo.Node()

	// Get pod and node labels
	podLabels := pod.Labels
	nodeLabels := node.Labels

	node_hostname, ln_exist := nodeLabels["kubernetes.io/hostname"]
	pod_hostname, lp_exist := podLabels["kubernetes.io/hostname"]

	log := fmt.Sprintf("Pod label: %s \n Node Label: %s ", pod_hostname, node_hostname)
	klog.V(0).Info(log)

	if !ln_exist || !lp_exist || node_hostname != pod_hostname {
		return framework.NewStatus(framework.Unschedulable, "Pod label does not match with node label")
	}

	return framework.NewStatus(framework.Success)
}
