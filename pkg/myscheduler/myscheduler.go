package myscheduler

import (
	"context"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	klog "k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// Constantes y estructuras de datos

var _ framework.PreFilterPlugin = &MyScheduler{}
var _ framework.FilterPlugin = &MyScheduler{}
var _ framework.ScorePlugin = &MyScheduler{}
var _ framework.ReservePlugin = &MyScheduler{}

const (
	preFilterStateKey      = "PodResources"
	Name                   = "MyScheduler"
	annotationKeyAssigned  = "resources.assigned/tflops"
	annotationKeyRequested = "resources.requested/tflops"
)

var nodeGpus *allNodesGpus = newAllNodesGpus()

var podsUsage *podsGpuUsage = newPodsGpuUsage()

type PreFilterState struct {
	resources framework.Resource
}

func (s *PreFilterState) Clone() framework.StateData {
	return s
}

// Plugin
type MyScheduler struct {
	handle    framework.Handle
	k8sClient *kubernetes.Clientset
}

func (m *MyScheduler) Name() string {
	return Name
}

func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	// Creacion del informer
	podInformer := h.SharedInformerFactory().Core().V1().Pods().Informer()

	podInformer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFunc,
		Handler: cache.ResourceEventHandlerFuncs{
			DeleteFunc: onDelete,
		},
	})
	// Inicializar la configuración de Kubernetes
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("error al obtener la configuración in-cluster: %w", err)
	}
	// Crear el cliente de la API K8s
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("error al crear el cliente de K8s: %w", err)
	}

	return &MyScheduler{
		handle:    h,
		k8sClient: clientset,
	}, nil
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
	if nodeName == "kwok-node-0" {
		score = 0
	}

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
	gpuPosition := 0
	gpuUsage := 1

	podsUsage.setPodResourcesGpuOnly(podName, name, gpuPosition, gpuUsage)

	return framework.NewStatus(framework.Success)
}

func (m *MyScheduler) Unreserve(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) {

}

// func (m *MyScheduler) PreBind(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {

// 	var tFlops int = 100
// 	intValueStr := strconv.Itoa(tFlops)

// 	// preFilterState, err := getPreFilterState(state)

// 	// if err != nil {
// 	// 	return framework.NewStatus(framework.Unschedulable, "Failed to read preFilterState from cycleState")
// 	// }

// 	patchPayload := fmt.Sprintf(`{"metadata":{"annotations":{"%s":"%s", "%s":"%s"}}}`, annotationKeyAssigned, intValueStr, annotationKeyRequested, "50")

// 	_, err := m.k8sClient.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.StrategicMergePatchType, []byte(patchPayload), metav1.PatchOptions{})

// 	if err != nil {
// 		return framework.NewStatus(framework.Error, fmt.Sprintf("Fallo al añadir la anotación en PreBind: %v", err))
// 	}
// 	return framework.NewStatus(framework.Success)
// }
