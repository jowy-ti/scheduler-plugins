package myscheduler

import (
	"context"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	klog "k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// Constantes y estructuras de datos

var _ framework.PreFilterPlugin = &MyScheduler{}
var _ framework.FilterPlugin = &MyScheduler{}
var _ framework.ScorePlugin = &MyScheduler{}
var _ framework.ReservePlugin = &MyScheduler{}
var _ framework.PostBindPlugin = &MyScheduler{}

const (
	preFilterStateKey = "resources"
	Name              = "MyScheduler"
)

var nodeGpus *allNodesGpus = newAllNodesGpus()

var podsUsage *podsGpuUsage = newPodsGpuUsage()

// StateData
type PreFilterState struct {
	resources framework.Resource
}

func (s *PreFilterState) Clone() framework.StateData {
	return s
}

// Plugin
type MyScheduler struct {
	handle framework.Handle
}

func (m *MyScheduler) Name() string {
	return Name
}

func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	podInformer := h.SharedInformerFactory().Core().V1().Pods().Informer()

	podInformer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFunc,
		Handler: cache.ResourceEventHandlerFuncs{
			DeleteFunc: onDelete,
		},
	})

	return &MyScheduler{handle: h}, nil
}

// Etapas scheduling

// Se cachea los recursos que pide el pod
func (m *MyScheduler) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	// state.SetRecordPluginMetrics(true)

	var podRequests *framework.Resource = computePodResourceRequest(pod)

	var preFilterState *PreFilterState = &PreFilterState{
		resources: *podRequests,
	}

	state.Write(preFilterStateKey, preFilterState)

	return nil, framework.NewStatus(framework.Success)
}

func (m *MyScheduler) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func (m *MyScheduler) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {

	// pod requests
	preFilterState, err := getPreFilterState(state)

	if err != nil {
		return framework.NewStatus(framework.Unschedulable, "Failed to read preFilterState from cycleState")
	}

	var nodeName string = nodeInfo.GetName()
	_, ok := nodeGpus.nodes[nodeName]
	if !ok {
		klog.V(0).Infof("Not found node %s", nodeName)
		err = gpuNodeBuild(nodeInfo)

		if err != nil {
			return framework.NewStatus(framework.Unschedulable, err.Error())
		}
	}
	if nodeName == "kwok-node-0" {
		scanNode(nodeName)
	}

	var podRequests *framework.Resource = &preFilterState.resources
	var nodeRequested *framework.Resource = nodeInfo.Requested
	var nodeAllocatable *framework.Resource = nodeInfo.Allocatable

	return enoughNodeResources(nodeAllocatable, nodeRequested, podRequests)
}

func (m *MyScheduler) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	nodeInfo, err := m.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)

	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}

	// pod requests
	preFilterState, err := getPreFilterState(state)

	if err != nil {
		return 0, framework.NewStatus(framework.Error, "Failed to read preFilterState from cycleState")
	}

	var podRequests *framework.Resource = &preFilterState.resources
	var nodeRequested *framework.Resource = nodeInfo.Requested
	var nodeAllocatable *framework.Resource = nodeInfo.Allocatable

	var nodeAvailable *framework.Resource = subtractionResources(nodeAllocatable, nodeRequested)

	var score int64 = scoreCpuMem(nodeAllocatable, nodeRequested, nodeAvailable, podRequests) + scoreGpu(nodeAvailable)

	// klog.V(0).Infof("%s %s score: %d", pod.Name, nodeName, score)

	return score, framework.NewStatus(framework.Success)
}

func (m *MyScheduler) ScoreExtensions() framework.ScoreExtensions {
	return m
}

func (m *MyScheduler) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	var MaxNodeScore float64 = float64(framework.MaxNodeScore)
	var MaxScore int64 = math.MinInt64

	for _, nodeScore := range scores {
		if nodeScore.Score > MaxScore {
			MaxScore = nodeScore.Score
		}
	}

	for i, nodeScore := range scores {
		scores[i].Score = int64(MaxNodeScore - (MaxNodeScore * (float64(nodeScore.Score) / float64(MaxScore))))
		// klog.V(0).Infof("%s %s Normalize: %d", pod.Name, scores[i].Name, scores[i].Score)
	}
	return framework.NewStatus(framework.Success)
}

func (m *MyScheduler) Reserve(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) *framework.Status {

	name := "kwok-node-0"
	podName := p.Name

	nodeGpus.nodes[name][0].Lock()
	nodeGpus.nodes[name][0].available -= 3
	klog.V(0).Infof("-------------Post available: %d", nodeGpus.nodes[name][0].available)
	nodeGpus.nodes[name][0].Unlock()

	tempNodeAssignedPod := nodeAssignedPod{
		nodeName:    name,
		mig:         false,
		gpuPosition: 0,
		gpuUsage:    3,
	}

	podsUsage.Lock()

	podsUsage.pods[podName] = tempNodeAssignedPod

	podsUsage.Unlock()

	return framework.NewStatus(framework.Success)
}

func (m *MyScheduler) Unreserve(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) {

}

func (m *MyScheduler) PostBind(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) {

}
