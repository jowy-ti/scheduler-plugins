package myscheduler

import (
	"context"
	"fmt"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	klog "k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// Constantes y estructuras de datos

var _ framework.PreFilterPlugin = &MyScheduler{}
var _ framework.FilterPlugin = &MyScheduler{}

// var _ framework.ScorePlugin = &MyScheduler{}
var _ framework.ReservePlugin = &MyScheduler{}
var _ framework.PreBindPlugin = &MyScheduler{}

const (
	preFilterStateKey     framework.StateKey = "PodResources"
	Name                  string             = "MyScheduler"
	annotationKeyAssigned string             = "deadline"
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

	preFilterState, err := getPreFilterState(state)

	if err != nil {
		return framework.NewStatus(framework.Unschedulable, "Fallo al leer 'preFilterState' en 'cycleState'")
	}

	var nodeName string = nodeInfo.GetName()
	_, ok := nodeGpus.nodes[nodeName]

	if !ok {
		klog.V(0).Infof("Not found node %s", nodeName)
		err := gpuNodeBuild(nodeInfo)

		if err != nil {
			return framework.NewStatus(framework.Unschedulable, err.Error())
		}
	}
	var podRequests *framework.Resource = &preFilterState.resources
	var availableNodeCpu int = int(nodeInfo.Allocatable.MilliCPU - nodeInfo.Requested.MilliCPU)
	var availableNodeMem int = int(nodeInfo.Allocatable.Memory - nodeInfo.Requested.Memory)

	return enoughNodeResources(nodeName, availableNodeCpu, availableNodeMem, podRequests)
}

func (m *MyScheduler) Reserve(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) *framework.Status {

	kwok0 := "kwok-node-0"
	kwok1 := "kwok-node-1"
	podName := p.Name
	gpuPosition := 0
	gpuUsage := 3
	migPosition := 0
	migUsage := 5

	err := podsUsage.setPodResourcesGpuOnly(podName, kwok0, gpuPosition, gpuUsage)
	if err != nil {
		klog.V(0).Infof("%v", err)
	}

	err = podsUsage.setPodResourcesMigOnly(podName, kwok1, gpuPosition, migPosition, migUsage)

	if err != nil {
		klog.V(0).Infof("%v", err)
	}
	scanPodUsage(podName)
	scanNode(kwok0)

	return framework.NewStatus(framework.Success)
}

func (m *MyScheduler) Unreserve(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) {
	podsUsage.cleanPodResources(p.Name)
}

func (m *MyScheduler) PreBind(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {

	// preFilterState, err := getPreFilterState(state)

	// if err != nil {
	// 	return framework.NewStatus(framework.Unschedulable, "Failed to read preFilterState from cycleState")
	// }

	var timeInt int = int(30 + time.Now().Unix())
	timeStr := strconv.Itoa(timeInt)

	patchPayload := fmt.Sprintf(`{"metadata":{"annotations":{"%s":"%s"}}}`, annotationKeyAssigned, timeStr)

	_, err := m.k8sClient.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.StrategicMergePatchType, []byte(patchPayload), metav1.PatchOptions{})

	if err != nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("Fallo al añadir la anotación en PreBind: %v", err))
	}
	return framework.NewStatus(framework.Success)
}
