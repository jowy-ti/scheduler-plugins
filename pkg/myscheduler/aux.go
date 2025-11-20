package myscheduler

import (
	"fmt"
	"strconv"
	"sync"

	v1 "k8s.io/api/core/v1"
	klog "k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// Mapa del uso de recursos de los pods
type allGpuUsage struct {
	sync.RWMutex
	pods map[string]nodeAssignedPod
}

// Nodo asignado al pod y info de utilización de gpu
type nodeAssignedPod struct {
	nodeName    string
	mig         bool
	gpuPosition int
	migPosition int
	migUsage    int
	gpuUsage    int
}

// Mapa con la disponibilidad de GPU de los nodos
type allNodesGpus struct {
	sync.RWMutex
	nodes map[string][]gpuSpec
}

// Informacion de GPU
type gpuSpec struct {
	sync.RWMutex
	available int // sobre 10
	mig       bool
	migSlices []migPartition
}

// Informacion de la particion de MIG
type migPartition struct {
	sync.RWMutex
	available int
	size      int
	mem       int64
}

const (
	migEnabled          string = "mig-enabled"
	migInstances        string = "mig-instances"
	gpuResourceName     string = "nvidia.com/gpu"
	maxAvailabilityGpu  int    = 10
	gpuMemory           string = "nvidia.com/gpu.memory"
	filterPodLabel      string = "app"
	filterPodLabelValue string = "fake-pod"
)

// Funciones auxiliares

// onDelete

func onDelete(obj interface{}) {
	pod, ok := obj.(*v1.Pod)

	if !ok {
		return
	}

	var podName string = pod.Name

	// podsUsage.RLock()
	// info, exists := podsUsage.pods[podName]
	// podsUsage.RUnlock()
	podsUsage.RLock()
	var usage int = podsUsage.pods[podName].gpuUsage
	var position int = podsUsage.pods[podName].gpuPosition
	var nodeName string = podsUsage.pods[podName].nodeName
	podsUsage.RUnlock()

	// nodeGpus.Lock()
	// defer nodeGpus.Unlock()

	// nodeGpusSlice, ok := nodeGpus.nodes[info.nodeName]
	// if !ok || info.gpuPosition < 0 || info.gpuPosition >= len(nodeGpusSlice) {
	//     klog.Errorf("onDelete: Nodo %s o posición %d inválida. No se pudo liberar el recurso.", info.nodeName, info.gpuPosition)
	//     return
	// }

	// // 🚨 Obtener el Lock de ESCRITURA de la GPU específica
	// gpuSpec := &nodeGpusSlice[info.gpuPosition]
	// gpuSpec.Lock()
	// defer gpuSpec.Unlock() // Se liberará al salir de la función

	// klog.V(0).Infof("onDelete: PodUsage: %d    NodeAvailable: %d", info.gpuUsage, gpuSpec.available)

	// // Liberación del recurso
	// gpuSpec.available += info.gpuUsage

	// klog.V(0).Infof("onDelete: NodeAvailablePostDelete: %d", gpuSpec.available)

	// // 4. (Opcional) Eliminar entrada de podsUsage si no se hace en otro sitio.
	// // Aunque ya liberamos los recursos, si quieres mantener consistencia:
	// podsUsage.Lock()
	// delete(podsUsage.pods, podName)
	// podsUsage.Unlock()
	nodeGpus.Lock()
	klog.V(0).Infof("+++++++++onDelete: PodUsage: %d    NodeAvailable: %d", usage, nodeGpus.nodes[nodeName][position].available)
	nodeGpus.nodes[nodeName][position].available += usage
	klog.V(0).Infof("+++++++++onDelete: NodeAvailablePostDelete: %d", nodeGpus.nodes[nodeName][position].available)
	nodeGpus.Unlock()
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

// Construccion de la estructura de datos en cache de los nodos

func gpuNodeBuild(nodeInfo *framework.NodeInfo) error {

	var nodeName string = nodeInfo.GetName()
	var nodeLabels map[string]string = nodeInfo.Node().Labels
	var gpuCount int64 = nodeInfo.Allocatable.ScalarResources[v1.ResourceName(gpuResourceName)]

	mig, ok := nodeLabels[migEnabled]
	if !ok {
		return fmt.Errorf("label %s not found in node %s", migEnabled, nodeName)
	}

	var gpus []gpuSpec = make([]gpuSpec, gpuCount)

	if mig == "false" {

		for i := int64(0); gpuCount > i; i++ {
			gpus[i] = gpuSpec{
				available: maxAvailabilityGpu,
				mig:       false,
			}
		}

	} else {

		labelMemoryGpu, ok := nodeLabels[gpuMemory]
		if !ok {
			return fmt.Errorf("label %s not found in node %s", gpuMemory, nodeName)
		}

		MemoryGpu, err := strconv.ParseInt(labelMemoryGpu, 10, 0)
		if err != nil {
			return fmt.Errorf("error to convert string to int for string %s in node %s", labelMemoryGpu, nodeName)
		}

		labelInstances, ok := nodeLabels[migInstances]
		if !ok {
			return fmt.Errorf("label %s not found in node %s", migInstances, nodeName)
		}

		numInstances, err := strconv.ParseInt(labelInstances, 10, 0)
		if err != nil {
			return fmt.Errorf("error to convert string to int for string %s in node %s", labelInstances, nodeName)
		}

		var migGeometry []migPartition = make([]migPartition, numInstances)

		for i := int64(0); numInstances > i; i++ {
			migGeometry[i] = migPartition{
				available: maxAvailabilityGpu,
				size:      1,
				mem:       MemoryGpu / numInstances,
			}
		}

		for i := int64(0); gpuCount > i; i++ {
			var clonedMigSlices []migPartition = make([]migPartition, numInstances)
			copy(clonedMigSlices, migGeometry)

			gpus[i] = gpuSpec{
				mig:       true,
				migSlices: clonedMigSlices,
			}
		}
	}

	nodeGpus.Lock()
	nodeGpus.nodes[nodeName] = gpus
	nodeGpus.Unlock()

	return nil
}

// Debugar informacion del nodo en cache
func scanNode(nodeName string) {

	nodeGpus.RLock()
	var nodeGpusCopy []gpuSpec = nodeGpus.nodes[nodeName]
	nodeGpus.RUnlock()

	gpuLenght := len(nodeGpusCopy)
	klog.V(0).Infof("gpuLenght: %d", gpuLenght)

	for i := 0; gpuLenght > i; i++ {
		var gpu *gpuSpec = &nodeGpusCopy[i]
		gpu.RLock()

		klog.V(0).Infof("gpu%d info:", i)
		klog.V(0).Infof("- available: %d", gpu.available)
		mig := gpu.mig

		var migSlices []migPartition = gpu.migSlices
		gpu.RUnlock()
		// klog.V(0).Infof("- mig: %t", mig)
		if mig {
			migLength := len(migSlices)
			for j := 0; migLength > j; j++ {
				var migSlice *migPartition = &migSlices[j]
				migSlice.RLock()

				klog.V(0).Info("mig slice info:")
				klog.V(0).Infof("- available: %d", migSlice.available)
				klog.V(0).Infof("- size: %d", migSlice.size)
				klog.V(0).Infof("- mem: %d", migSlice.mem)

				migSlice.RUnlock()
			}
		}
	}
}

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

// // Dynamic Client
// 	dynamicClient, err := dynamic.NewForConfig(m.handle.KubeConfig())
// 	if err != nil {
// 		klog.V(0).Infof("Error: %v", err)
// 	}

// 	// GVR
// 	var gvr schema.GroupVersionResource = schema.GroupVersionResource{Group: "gpu.com", Version: "v1", Resource: "specifications"}

// 	// Acceso a CR
// 	cr, err := dynamicClient.Resource(gvr).Namespace("default").Get(context.TODO(), "example", metav1.GetOptions{})

// 	if err != nil {
// 		klog.V(0).Infof("Error: %v", err)
// 	}

// 	gpus, find, err := unstructured.NestedSlice(cr.UnstructuredContent(), "spec", "gpus")

// 	if !find {
// 		klog.V(0).Infof("Not found gpus")
// 	} else if err != nil {
// 		klog.V(0).Infof("Error: %v", err)
// 	}

// 	gpu1 := gpus[0]
// 	gpu1map, ok := gpu1.(map[string]interface{})
// 	if !ok {
// 		klog.V(0).Infof("Error de conversión de tipos")
// 	}
// 	available := gpu1map["available"]
// 	availableint := available.(int64)

// 	klog.V(0).Infof("Available: %d", availableint)
