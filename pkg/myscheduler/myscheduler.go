package myscheduler

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	klog "k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// Plugin
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
var _ framework.ScorePlugin = &MyScheduler{}

const (
	preFilterStateKey = "resources"
)

//////////////////////////////////////////////////////////////////////////////////

// StateData
type PreFilterState struct {
	resources framework.Resource
}

func (s *PreFilterState) Clone() framework.StateData {
	return s
}

//////////////////////////////////////////////////////////////////////////////////

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

	state.Write(preFilterStateKey, preFilterState)

	// klog.V(0).Infof("nvidia.com/mig-1g.6gb: %d ", podResources.ScalarResources["nvidia.com/mig-1g.6gb"])
	// klog.V(0).Infof("nvidia.com/mig-2g.12gb: %d ", podResources.ScalarResources["nvidia.com/mig-2g.12gb"])
	// klog.V(0).Infof("nvidia.com/mig-2g.20gb: %d ", podResources.ScalarResources["nvidia.com/mig-2g.20gb"])
	// klog.V(0).Infof("nvidia.com/mig-3g.40gb: %d ", podResources.ScalarResources["nvidia.com/mig-3g.40gb"])
	// klog.V(0).Infof("nvidia.com/mig-2g.35gb: %d ", podResources.ScalarResources["nvidia.com/mig-2g.35gb"])
	// klog.V(0).Infof("nvidia.com/mig-3g.71gb: %d ", podResources.ScalarResources["nvidia.com/mig-3g.71gb"])

	return nil, framework.NewStatus(framework.Success)
}

func (m *MyScheduler) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func (m *MyScheduler) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	nodeInfo, err := m.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)

	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}

	preFilterState, err := getPreFilterState(state)

	if err != nil {
		return 0, framework.NewStatus(framework.Error, "Failed to read preFilterState from cycleState")
	}

	podRequests := preFilterState.resources
	nodeRequested := nodeInfo.Requested
	nodeAllocatable := nodeInfo.Allocatable

	resourcesAvailable := subtractionResources(nodeAllocatable, nodeRequested)
	resourcesLeft := subtractionResources(resourcesAvailable, &podRequests)

	klog.V(0).Infof("%s CPU: %d ", nodeName, resourcesLeft.MilliCPU)
	klog.V(0).Infof("%s MEM: %d ", nodeName, resourcesLeft.Memory)
	klog.V(0).Infof("%s nvidia.com/mig-1g.6gb: %d ", nodeName, resourcesLeft.ScalarResources["nvidia.com/mig-1g.6gb"])
	klog.V(0).Infof("%s nvidia.com/mig-2g.12gb: %d ", nodeName, resourcesLeft.ScalarResources["nvidia.com/mig-2g.12gb"])

	return 10, framework.NewStatus(framework.Success)
}

func (m *MyScheduler) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

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

func getPreFilterState(cycleState *framework.CycleState) (*PreFilterState, error) {
	stateData, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		// preFilterState doesn't exist, likely PreFilter wasn't invoked.
		return nil, fmt.Errorf("error reading %q from cycleState: %w", preFilterStateKey, err)
	}

	preFilterState, ok := stateData.(*PreFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to NodeResourcesFit.preFilterState error", stateData)
	}
	return preFilterState, nil
}

func subtractionResources(Allocatable *framework.Resource, Requested *framework.Resource) *framework.Resource {
	result := &framework.Resource{}

	result.MilliCPU = Allocatable.MilliCPU - Requested.MilliCPU
	result.Memory = Allocatable.Memory - Requested.Memory

	if Allocatable.ScalarResources == nil {
		return result
	}

	result.ScalarResources = make(map[v1.ResourceName]int64)

	for resourceNameAllocatable, quantityAllocatable := range Allocatable.ScalarResources {
		result.ScalarResources[resourceNameAllocatable] = quantityAllocatable - Requested.ScalarResources[resourceNameAllocatable]
	}

	return result
}

// func (m *MyScheduler) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
// 	nodeRequested := nodeInfo.Requested
// 	nodeAllocatable := nodeInfo.Allocatable

// 	klog.V(0).Infof("%s Utilized: nvidia.com/mig-1g.6gb: %d ", nodeInfo.GetName(), nodeRequested.ScalarResources["nvidia.com/mig-1g.6gb"])
// 	klog.V(0).Infof("%s Utilized: nvidia.com/mig-2g.12gb: %d ", nodeInfo.GetName(), nodeRequested.ScalarResources["nvidia.com/mig-2g.12gb"])
// 	klog.V(0).Infof("%s Allocatable: nvidia.com/mig-1g.6gb: %d ", nodeInfo.GetName(), nodeAllocatable.ScalarResources["nvidia.com/mig-1g.6gb"])
// 	klog.V(0).Infof("%s Allocatable: nvidia.com/mig-2g.12gb: %d ", nodeInfo.GetName(), nodeAllocatable.ScalarResources["nvidia.com/mig-2g.12gb"])

// 	// Pulling pod requests
// 	stateData, err := state.Read("resources")

// 	if err != nil {
// 		return framework.NewStatus(framework.Unschedulable, "Error getting resources")
// 	}

// 	podResources, ok := stateData.(*PreFilterState)

// 	if !ok {
// 		return framework.NewStatus(framework.Unschedulable, "Error getting resources 2")
// 	}

// 	klog.V(0).Infof("%s Requests: nvidia.com/mig-1g.6gb: %d ", pod.Name, podResources.resources.ScalarResources["nvidia.com/mig-1g.6gb"])
// 	klog.V(0).Infof("%s Requests: nvidia.com/mig-2g.12gb: %d ", pod.Name, podResources.resources.ScalarResources["nvidia.com/mig-2g.12gb"])

// 	// for resource, amount := range podResources.resources.ScalarResources {

// 	// }
// 	return framework.NewStatus(framework.Success)
// }
