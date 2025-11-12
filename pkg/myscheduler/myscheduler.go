package myscheduler

import (
	"context"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	unstructured "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
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

// Constantes y estructuras de datos

var _ framework.PreFilterPlugin = &MyScheduler{}
var _ framework.FilterPlugin = &MyScheduler{}
var _ framework.ScorePlugin = &MyScheduler{}

const (
	preFilterStateKey = "resources"
)

// StateData
type PreFilterState struct {
	resources framework.Resource
}

func (s *PreFilterState) Clone() framework.StateData {
	return s
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

	// // Dynamic Client
	dynamicClient, err := dynamic.NewForConfig(m.handle.KubeConfig())
	if err != nil {
		klog.V(0).Infof("Error: %v", err)
	}

	// GVR
	var gvr schema.GroupVersionResource = schema.GroupVersionResource{Group: "gpu.com", Version: "v1", Resource: "specifications"}

	// Acceso a CR
	cr, err := dynamicClient.Resource(gvr).Namespace("default").Get(context.TODO(), "nombre-de-mi-cr", metav1.GetOptions{})

	if err != nil {
		klog.V(0).Infof("Error: %v", err)
	}

	gpus, find, err := unstructured.NestedSlice(cr.UnstructuredContent(), "spec", "gpus")

	if !find {
		klog.V(0).Infof("Not found gpus")
	} else if err != nil {
		klog.V(0).Infof("Error: %v", err)
	}

	gpu1 := gpus[0]
	gpu1map, ok := gpu1.(map[string]interface{})
	available := gpu1map["available"]
	availableint := available.(int64)

	klog.V(0).Infof("Available: %d", availableint)

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

	klog.V(0).Infof("%s %s score: %d", pod.Name, nodeName, score)

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
		klog.V(0).Infof("%s %s Normalize: %d", pod.Name, scores[i].Name, scores[i].Score)
	}
	return framework.NewStatus(framework.Success)
}
