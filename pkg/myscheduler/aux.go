package myscheduler

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
	// unstructured "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

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
