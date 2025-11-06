package myscheduler

import (
	"context"
	"fmt"
	"math"

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

// Funciones auxiliares

// Computa el total de recursos que solicita un pod
func computePodResourceRequest(pod *v1.Pod) *framework.Resource {
	var result *framework.Resource = &framework.Resource{}

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

// Obtencion del StateData dentro del CycleState
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

// Recursos disponibles
func subtractionResources(allocatable *framework.Resource, requested *framework.Resource) *framework.Resource {
	var result *framework.Resource = &framework.Resource{}

	result.MilliCPU = allocatable.MilliCPU - requested.MilliCPU
	result.Memory = allocatable.Memory - requested.Memory

	if allocatable.ScalarResources == nil {
		return result
	}

	result.ScalarResources = make(map[v1.ResourceName]int64)

	for resourceNameAllocatable, quantityAllocatable := range allocatable.ScalarResources {
		result.ScalarResources[resourceNameAllocatable] = quantityAllocatable - requested.ScalarResources[resourceNameAllocatable]
	}

	return result
}

// Verifica si el nodo tiene recursos suficientes para el pod
func enoughNodeResources(nodeAllocatable *framework.Resource, nodeRequested *framework.Resource, podRequests *framework.Resource) *framework.Status {

	var resourcesAvailable *framework.Resource = subtractionResources(nodeAllocatable, nodeRequested)

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

// Puntuación de la cpu y memoria
func scoreCpuMem(nodeAllocatable *framework.Resource, nodeRequested *framework.Resource, nodeAvailable *framework.Resource, podRequests *framework.Resource) int64 {
	const weightMem int64 = 1 << 20         // valor de la heuristica
	const penalizationBalance float64 = 2.0 // mayor número penaliza menos el desbalance, menor penaliza más. Rango de valores posibles (1, inf) // valor de la heuristica

	// Relative
	var memRel float64 = float64(nodeRequested.Memory+podRequests.Memory) / float64(nodeAllocatable.Memory)
	var cpuRel float64 = float64(nodeRequested.MilliCPU+podRequests.MilliCPU) / float64(nodeAllocatable.MilliCPU)
	var relCpuMem float64 = memRel - cpuRel

	if relCpuMem < 0 {
		relCpuMem = -1.0 * relCpuMem
	}

	var balanceCpuMem float64 = 1.0 - (relCpuMem / penalizationBalance) // Intervalo de menos a más balanceado (0.5, 1)

	// Absolute
	var weightedCpuMem int64 = (nodeAvailable.Memory / weightMem) + nodeAvailable.MilliCPU

	// Result
	var resCpuMem float64 = float64(weightedCpuMem) / balanceCpuMem
	// klog.V(0).Infof("Resources: %d     Balance: %f", weightedCpuMem, balanceCpuMem)
	return int64(resCpuMem)
}

// Puntuación de la GPU
func scoreGpu(nodeAvailable *framework.Resource) int64 {
	const weightGpu int64 = 1 << 13 // valor de la heuristica
	var resGpu int64 = 0

	if nodeAvailable.ScalarResources == nil {
		return resGpu
	}

	for _, quantityAvailable := range nodeAvailable.ScalarResources {
		resGpu += quantityAvailable * weightGpu
	}

	return resGpu
}
