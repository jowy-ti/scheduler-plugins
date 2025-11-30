package myscheduler

import (
	"fmt"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	migEnabled          string = "mig-enabled"
	migInstances        string = "mig-instances"
	gpuResourceName     string = "nvidia.com/gpu"
	maxAvailabilityGpu  int    = 10
	gpuMemory           string = "nvidia.com/gpu.memory"
	gpufp32GFLOPS       string = "nvidia.com/gpu.fp32.GFLOPS"
	podRequestGpuMemory string = "customresource.com/gpuMemory"
	podRequestGpufp32   string = "customresource.com/gpufp32"
	filterPodLabel      string = "app"
	filterPodLabelValue string = "fake-pod"
)

// Funciones auxiliares

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
	var podName string = pod.Name
	podsUsage.cleanPodResources(podName)
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

	labelMemoryGpu, ok := nodeLabels[gpuMemory]
	if !ok {
		return fmt.Errorf("label %s not found in node %s", gpuMemory, nodeName)
	}

	memoryGpu, err := strconv.ParseInt(labelMemoryGpu, 10, 0)
	if err != nil {
		return fmt.Errorf("error to convert string to int for string %s in node %s", labelMemoryGpu, nodeName)
	}

	labelfp32Gpu, ok := nodeLabels[gpufp32GFLOPS]
	if !ok {
		return fmt.Errorf("label %s not found in node %s", gpufp32GFLOPS, nodeName)
	}

	fp32Gpu, err := strconv.ParseInt(labelfp32Gpu, 10, 0)
	if err != nil {
		return fmt.Errorf("error to convert string to int for string %s in node %s", labelfp32Gpu, nodeName)
	}

	labelMig, ok := nodeLabels[migEnabled]
	if !ok {
		return fmt.Errorf("label %s not found in node %s", migEnabled, nodeName)
	}

	mig, err := strconv.ParseBool(labelMig)
	if err != nil {
		return fmt.Errorf("error to convert string to bool for string %s in node %s", labelMig, nodeName)
	}

	labelInstances, ok := nodeLabels[migInstances]
	if !ok {
		return fmt.Errorf("label %s not found in node %s", migInstances, nodeName)
	}
	numInstances, err := strconv.ParseInt(labelInstances, 10, 0)

	if err != nil {
		return fmt.Errorf("error to convert string to int for string %s in node %s", labelInstances, nodeName)
	}

	var gpus []*gpuSpec = make([]*gpuSpec, gpuCount)

	for i := int64(0); gpuCount > i; i++ {
		gpus[i] = newGpuSpec()
		gpus[i].setGpuSpecGpuOnly(int(memoryGpu), int(fp32Gpu), mig, int(numInstances))
	}

	if mig {
		for i := 0; int(gpuCount) > i; i++ {
			var migGeometry []*migSlice = make([]*migSlice, int(numInstances))
			migGeometry[0] = newMigSlice()
			migGeometry[0].setInfoMigSlice(int(numInstances), int(memoryGpu), int(fp32Gpu))
			gpus[i].setGpuSpecMigOnly(migGeometry)
		}
	}

	nodeGpus.setAllNodesGpus(gpus, nodeName)
	return nil
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
func enoughNodeResources(nodeName string, availableNodeCpu int, availableNodeMem int, podRequests *framework.Resource) *framework.Status {

	if availableNodeCpu < int(podRequests.MilliCPU) {
		return framework.NewStatus(framework.Unschedulable, "Insufficient CPU")
	}
	if availableNodeMem < int(podRequests.Memory) {
		return framework.NewStatus(framework.Unschedulable, "Insufficient Memory")
	}

	totalGpus, err := nodeGpus.getLength(nodeName)

	if err != nil {
		return framework.NewStatus(framework.Unschedulable, err.Error())
	}

	for i := 0; totalGpus > i; i++ {
		gpu, err := nodeGpus.getGeneralGpuResources(nodeName, i)
		if err != nil {
			return framework.NewStatus(framework.Unschedulable, err.Error())
		}

		if !gpu.mig {
			var gpuAvailable float64 = float64(gpu.available) / 10.0
			var gpuMemAvailable int = int(gpuAvailable * float64(gpu.mem))
			var gpuFp32Available int = int(gpuAvailable * float64(gpu.fp32))

			if gpuMemAvailable > int(podRequests.ScalarResources[v1.ResourceName(podRequestGpuMemory)]) && gpuFp32Available > int(podRequests.ScalarResources[v1.ResourceName(podRequestGpufp32)]) {
				return framework.NewStatus(framework.Success)
			}
		} else {
			for j := 0; gpu.migLength > j; j++ {
				migPartition, err := gpu.getMigSlice(j)
				if err != nil {
					return framework.NewStatus(framework.Unschedulable, err.Error())
				}

				var migAvailable float64 = float64(migPartition.available) / 10.0
				var migMemAvailable int = int(migAvailable * float64(migPartition.mem))
				var migFp32Available int = int(migAvailable * float64(migPartition.fp32))

				if migMemAvailable > int(podRequests.ScalarResources[v1.ResourceName(podRequestGpuMemory)]) && migFp32Available > int(podRequests.ScalarResources[v1.ResourceName(podRequestGpufp32)]) {
					return framework.NewStatus(framework.Success)
				}

				j += migPartition.size - 1
			}
		}
	}

	return framework.NewStatus(framework.Unschedulable, "Insufficient resources")
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
	const weightMem int = 1 << 20           // valor de la heuristica
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
	var weightedCpuMem int = (int(nodeAvailable.Memory) / weightMem) + int(nodeAvailable.MilliCPU)

	// Result
	var resCpuMem float64 = float64(weightedCpuMem) / balanceCpuMem
	// klog.V(0).Infof("Resources: %d     Balance: %f", weightedCpuMem, balanceCpuMem)
	return int64(resCpuMem)
}

// Puntuación de la GPU
func scoreGpu(nodeAvailable *framework.Resource) int64 {
	const weightGpu int = 1 << 13 // valor de la heuristica
	var resGpu int = 0

	if nodeAvailable.ScalarResources == nil {
		return int64(resGpu)
	}

	for _, quantityAvailable := range nodeAvailable.ScalarResources {
		resGpu += int(quantityAvailable) * weightGpu
	}

	return int64(resGpu)
}
