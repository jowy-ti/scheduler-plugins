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
	"k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// Constantes y estructuras de datos

var _ framework.PreFilterPlugin = &MyScheduler{}
var _ framework.FilterPlugin = &MyScheduler{}
var _ framework.ScorePlugin = &MyScheduler{}
var _ framework.ReservePlugin = &MyScheduler{}
var _ framework.PreBindPlugin = &MyScheduler{}

const (
	preFilterStateKey   framework.StateKey = "PodResources"
	Name                string             = "MyScheduler"
	timePodAssigned     string             = "realDeletionTime"
	scheduledAnnotation string             = "customresource.com/scheduled-time"
	deletionAnnotation  string             = "customresource.com/deletion-time"
	hardIsolation       string             = "hardIsolation"
	podDurationName     string             = "schedulingDuration"
)

var nodeGpus *allNodesGpus = newAllNodesGpus()
var podsUsage *podsGpuUsage = newPodsGpuUsage()

type PreFilterState struct {
	resources         framework.Resource
	hardwareIsolation bool
	assignation       string
	startTime         time.Time
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
	var podAnnotations map[string]string = pod.Annotations

	hardwareIsolationAnnotation, ok := podAnnotations[hardIsolation]
	if !ok {
		return nil, framework.NewStatus(framework.Unschedulable, fmt.Sprintf("No existe la anotación %s", hardIsolation))
	}

	hardwareIsolation, err := strconv.ParseBool(hardwareIsolationAnnotation)
	if err != nil {
		return nil, framework.NewStatus(framework.Unschedulable, err.Error())
	}

	var preFilterState *PreFilterState = &PreFilterState{
		resources:         *podRequests,
		hardwareIsolation: hardwareIsolation,
		startTime:         time.Now(),
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
	var gpuCount int = int(nodeInfo.Allocatable.ScalarResources[gpuResourceName])
	var nodeLabels map[string]string = nodeInfo.Node().Labels
	_, ok := nodeGpus.nodes[nodeName]

	if !ok {
		memoryGpu, fp32Gpu, numInstances, err := extractNodeInfo(nodeLabels, nodeName)

		if err != nil {
			return framework.NewStatus(framework.Unschedulable, err.Error())
		}

		err = gpuNodeBuild(nodeName, memoryGpu, fp32Gpu, numInstances, gpuCount)

		if err != nil {
			return framework.NewStatus(framework.Unschedulable, err.Error())
		}
	}

	var hardwareIsolation bool = preFilterState.hardwareIsolation
	var podRequests *framework.Resource = &preFilterState.resources
	var availableNodeCpu int = int(nodeInfo.Allocatable.MilliCPU - nodeInfo.Requested.MilliCPU)
	var availableNodeMem int = int(nodeInfo.Allocatable.Memory - nodeInfo.Requested.Memory)

	return enoughNodeResources(nodeName, availableNodeCpu, availableNodeMem, podRequests, hardwareIsolation)
}

func (m *MyScheduler) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {

	score, status := nodeGpusUsage(nodeName)
	klog.V(0).Infof("%s score: %d", nodeName, score)
	return score, status
}

func (m *MyScheduler) ScoreExtensions() framework.ScoreExtensions {
	return m
}

func (m *MyScheduler) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	var MaxScore int64 = framework.MinNodeScore

	for _, nodeScore := range scores {
		if nodeScore.Score > MaxScore {
			MaxScore = nodeScore.Score
		}
	}

	if MaxScore == 0 {
		return framework.NewStatus(framework.Success)
	}

	for i, nodeScore := range scores {
		scores[i].Score = int64(float64(framework.MaxNodeScore) * (float64(nodeScore.Score) / float64(MaxScore)))
	}
	return framework.NewStatus(framework.Success)
}

func (m *MyScheduler) Reserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {

	var gpuAssigned string
	preFilterState, err := getPreFilterState(state)

	if err != nil {
		return framework.NewStatus(framework.Unschedulable, "Fallo al leer 'preFilterState' en 'cycleState'")
	}

	var hardwareIsolation bool = preFilterState.hardwareIsolation
	var podRequests *framework.Resource = &preFilterState.resources
	mig, err := nodeGpus.isMig(nodeName)

	if err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	}

	if !mig {
		gpuAssigned, err = gpuReservation(pod.Name, nodeName, podRequests, hardwareIsolation)
	} else {
		gpuAssigned, err = migReservation(pod.Name, nodeName, podRequests, hardwareIsolation)
	}

	if err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	}

	var newPreFilterState *PreFilterState = &PreFilterState{
		resources:         preFilterState.resources,
		hardwareIsolation: preFilterState.hardwareIsolation,
		assignation:       gpuAssigned,
		startTime:         preFilterState.startTime,
	}

	state.Write(preFilterStateKey, newPreFilterState)

	// scanPodUsage(pod.Name)
	scanNode(nodeName)

	return framework.NewStatus(framework.Success)
}

func (m *MyScheduler) Unreserve(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) {
	podsUsage.cleanPodResources(p.Name)
}

func (m *MyScheduler) PreBind(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {

	var podAnnotations map[string]string = pod.Annotations

	scheduledTime, deletionTime, err := extractPodTimes(podAnnotations, pod.Name)

	if err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	}

	preFilterState, err := getPreFilterState(state)

	if err != nil {
		return framework.NewStatus(framework.Unschedulable, "Fallo al leer 'preFilterState' en 'cycleState'")
	}

	var interval int64 = deletionTime - scheduledTime
	var finalTimeInt int64 = interval + time.Now().Unix()
	var finaTimeStr string = strconv.Itoa(int(finalTimeInt))
	var schedulingDuration int = int(time.Since(preFilterState.startTime).Milliseconds())
	var patchPayload string = fmt.Sprintf(`{"metadata":{"annotations":{"%s":"%d","%s":"%s",%s}}}`, podDurationName, schedulingDuration, timePodAssigned, finaTimeStr, preFilterState.assignation)

	_, err = m.k8sClient.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.StrategicMergePatchType, []byte(patchPayload), metav1.PatchOptions{})

	if err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	}

	return framework.NewStatus(framework.Success)
}

// onDelete

func onDelete(obj interface{}) {
	pod, ok := obj.(*v1.Pod)

	if !ok {
		unknown, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		pod, ok = unknown.Obj.(*v1.Pod)
		if !ok {
			return
		}
	}
	// klog.V(0).Infof("Deleted pod: %s", pod.Name)
	podsUsage.cleanPodResources(pod.Name)
}

// FilterFunc

func filterFunc(obj interface{}) bool {
	pod, ok := obj.(*v1.Pod)

	if !ok {
		return false
	}
	var podLabels map[string]string = pod.Labels
	return podLabels[filterPodLabel] == filterPodLabelValue
}
