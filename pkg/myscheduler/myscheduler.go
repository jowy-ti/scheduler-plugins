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

var _ framework.PreFilterPlugin = &MyScheduler{}
var _ framework.FilterPlugin = &MyScheduler{}

type PreFilterState struct {
	resources framework.Resource
}

func (s *PreFilterState) Clone() framework.StateData {
	return s
}

func (m *MyScheduler) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {

	state.SetRecordPluginMetrics(true)
	podLabels := pod.Labels

	if podLabels == nil {
		return nil, framework.NewStatus(framework.Unschedulable, "No labels in the Pod")
	}

	podApp, podLabelExist := podLabels["app"]

	// Filtrar fake pods
	if !podLabelExist || podApp != "fake-pod" {
		return nil, framework.NewStatus(framework.Unschedulable, "No fake Pod")
	}

	podResources := computePodResourceRequest(pod)

	preFilterState := &PreFilterState{
		resources: *podResources,
	}

	state.Write("resources", preFilterState)

	klog.V(0).Infof("nvidia.com/mig-1g.6gb: %d ", podResources.ScalarResources["nvidia.com/mig-1g.6gb"])
	klog.V(0).Infof("nvidia.com/mig-2g.12gb: %d ", podResources.ScalarResources["nvidia.com/mig-2g.12gb"])
	klog.V(0).Infof("nvidia.com/mig-2g.20gb: %d ", podResources.ScalarResources["nvidia.com/mig-2g.20gb"])
	klog.V(0).Infof("nvidia.com/mig-3g.40gb: %d ", podResources.ScalarResources["nvidia.com/mig-3g.40gb"])
	klog.V(0).Infof("nvidia.com/mig-2g.35gb: %d ", podResources.ScalarResources["nvidia.com/mig-2g.35gb"])
	klog.V(0).Infof("nvidia.com/mig-3g.71gb: %d ", podResources.ScalarResources["nvidia.com/mig-3g.71gb"])

	return nil, framework.NewStatus(framework.Success)
}

func (m *MyScheduler) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func (m *MyScheduler) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {

	node := nodeInfo.Node()

	nodeName := node.Name
	nodeLabels := node.Labels

	if nodeLabels == nil {
		return framework.NewStatus(framework.Unschedulable, "No labels in the Node")
	}

	// Filter nodes GPU present

	nodeGPU, nodeLabelExist := nodeLabels["nvidia.com/gpu.present"]

	if !nodeLabelExist || nodeGPU != "true" {
		return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("node %s label 'nvidia.com/gpu.present': %s ", nodeName, nodeGPU))
	}

	resources, err := state.Read("resources")

	if err != nil {
		// preFilterState doesn't exist, likely PreFilter wasn't invoked.
		return framework.NewStatus(framework.Unschedulable, "Error getting resources")
	}

	prefilterState, ok := resources.(*PreFilterState)

	if !ok {
		return framework.NewStatus(framework.Unschedulable, "Error getting resources 2")
	}

	klog.V(0).Infof("Filter: nvidia.com/mig-1g.6gb: %d ", prefilterState.resources.ScalarResources["nvidia.com/mig-1g.6gb"])
	klog.V(0).Infof("Filter: nvidia.com/mig-2g.12gb: %d ", prefilterState.resources.ScalarResources["nvidia.com/mig-2g.12gb"])

	return framework.NewStatus(framework.Success)
}

// func (m *MyScheduler) Score(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (int64, *framework.Status) {
// 	return 10, framework.NewStatus(framework.Success)
// }

// func (m *MyScheduler) ScoreExtensions() framework.ScoreExtensions {
// 	return nil
// }

// func (m *MyScheduler) NormalizeScore(ctx context.Context, state *framework.CycleState, p *v1.Pod, scores framework.NodeScoreList) *framework.Status {

// }

func computePodResourceRequest(pod *v1.Pod) *framework.Resource {
	result := &framework.Resource{}
	for _, container := range pod.Spec.Containers {
		result.Add(container.Resources.Requests)
	}

	// take max_resource(sum_pod, any_init_container)
	for _, container := range pod.Spec.InitContainers {
		result.SetMaxResource(container.Resources.Requests)
	}

	// If Overhead is being utilized, add to the total requests for the pod
	if pod.Spec.Overhead != nil {
		result.Add(pod.Spec.Overhead)
	}

	return result
}
