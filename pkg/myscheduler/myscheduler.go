package myscheduler

import (
	"context"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
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
var _ framework.FilterPlugin = &MyScheduler{}
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

	// state.SetRecordPluginMetrics(true)

	podResources := computePodResourceRequest(pod)

	preFilterState := &PreFilterState{
		resources: *podResources,
	}

	state.Write(preFilterStateKey, preFilterState)

	return nil, framework.NewStatus(framework.Success)
}

func (m *MyScheduler) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

//////////////////////////////////////////////////////////////////////////////////

func (m *MyScheduler) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {

	// Pulling pod requests
	preFilterState, err := getPreFilterState(state)

	if err != nil {
		return framework.NewStatus(framework.Unschedulable, "Failed to read preFilterState from cycleState")
	}

	podRequests := &preFilterState.resources
	nodeRequested := nodeInfo.Requested
	nodeAllocatable := nodeInfo.Allocatable

	resourcesAvailable := subtractionResources(nodeAllocatable, nodeRequested)

	if resourcesAvailable.MilliCPU < podRequests.MilliCPU {
		return framework.NewStatus(framework.Unschedulable, "Insufficient CPU")
	}

	if resourcesAvailable.Memory < podRequests.Memory {
		return framework.NewStatus(framework.Unschedulable, "Insufficient Memory")
	}

	for resourceName, amount := range podRequests.ScalarResources {
		if resourcesAvailable.ScalarResources[resourceName] < amount {
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Insufficient %s", resourceName.String()))
		}
	}
	return framework.NewStatus(framework.Success)
}

//////////////////////////////////////////////////////////////////////////////////

func (m *MyScheduler) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	nodeInfo, err := m.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)

	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}

	// Pulling pod requests
	preFilterState, err := getPreFilterState(state)

	if err != nil {
		return 0, framework.NewStatus(framework.Error, "Failed to read preFilterState from cycleState")
	}

	podRequests := &preFilterState.resources
	nodeRequested := nodeInfo.Requested
	nodeAllocatable := nodeInfo.Allocatable

	// klog.V(0).Infof("%s CPU: %d ", nodeName, resourcesLeft.MilliCPU)
	// klog.V(0).Infof("%s MEM: %d ", nodeName, resourcesLeft.Memory)
	// klog.V(0).Infof("%s nvidia.com/mig-1g.6gb: %d ", nodeName, resourcesLeft.ScalarResources["nvidia.com/mig-1g.6gb"])
	// klog.V(0).Infof("%s nvidia.com/mig-2g.12gb: %d ", nodeName, resourcesLeft.ScalarResources["nvidia.com/mig-2g.12gb"])

	scoreCpuMem := scoreCpuMem(nodeAllocatable, nodeRequested, podRequests)
	scoreGpu()

	klog.V(0).Infof("%s resCpuMem: %f", nodeName, scoreCpuMem)

	return 50, framework.NewStatus(framework.Success)
}

func (m *MyScheduler) ScoreExtensions() framework.ScoreExtensions {
	return m
}

func (m *MyScheduler) NormalizeScore(ctx context.Context, state *framework.CycleState, p *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	// framework.MaxNodeScore
	var MaxScore int64 = math.MinInt64

	for _, nodeScore := range scores {
		if nodeScore.Score > MaxScore {
			MaxScore = nodeScore.Score
		}
	}

	for i, nodeScore := range scores {
		scores[i].Score = int64(100.0 - (100.0 * (float64(nodeScore.Score) / float64(MaxScore))))
	}
	return framework.NewStatus(framework.Success)
}

//////////////////////////////////////////////////////////////////////////////////

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

func scoreCpuMem(nodeAllocatable *framework.Resource, nodeRequested *framework.Resource, podRequests *framework.Resource) float64 {
	var weightMem int64 = 1 << 20

	// Relative
	memRel := float64(nodeRequested.Memory+podRequests.Memory) / float64(nodeAllocatable.Memory)
	cpuRel := float64(nodeRequested.MilliCPU+podRequests.MilliCPU) / float64(nodeAllocatable.MilliCPU)
	res := memRel - cpuRel

	if res < 0 {
		res = -1.0 * res
	}

	balanceCpuMem := 1.0 - (res / 2.0)

	// Absolute
	resourcesAvailable := subtractionResources(nodeAllocatable, nodeRequested)

	memRes := resourcesAvailable.Memory / weightMem
	weightedCpuMem := memRes + resourcesAvailable.MilliCPU

	// Result
	resCpuMem := float64(weightedCpuMem) * float64(balanceCpuMem)

	return resCpuMem
}

func scoreGpu() float64 {

}
