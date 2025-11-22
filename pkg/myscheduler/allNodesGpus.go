package myscheduler

import (
	"fmt"
	"sync"
)

// Mapa con la disponibilidad de GPU de los nodos
type allNodesGpus struct {
	sync.RWMutex
	nodes map[string][]*gpuSpec
}

// Informacion de GPU
type gpuSpec struct {
	sync.RWMutex
	available int // sobre 10
	mig       bool
	migSlices []*migPartition
}

// Informacion de la particion de MIG
type migPartition struct {
	sync.RWMutex
	available int
	size      int
	mem       int64
}

func newAllNodesGpus() *allNodesGpus {
	return &allNodesGpus{
		nodes: make(map[string][]*gpuSpec),
	}
}

func (p *allNodesGpus) getMig(nodeName string, gpuPosition int) (bool, error) {

	p.RLock()
	defer p.RUnlock()
	gpus, ok := p.nodes[nodeName]

	if !ok {
		return false, fmt.Errorf("allNodesGpus.getMig: no existe el nodo con nombre %s", nodeName)
	}

	if gpuPosition >= len(gpus) || gpuPosition < 0 {
		return false, fmt.Errorf("allNodesGpus.getMig: posición de la gpu fuera de rango %d", gpuPosition)
	}

	var gpu *gpuSpec = gpus[gpuPosition]

	gpu.RLock()
	defer gpu.RUnlock()
	var mig bool = gpu.mig

	return mig, nil
}

func (p *allNodesGpus) ReserveResourcesGpuOnly(nodeName string, gpuPosition int, gpuUsage int) error {

	p.RLock()
	defer p.RUnlock()

	if _, exists := p.nodes[nodeName]; !exists {
		return fmt.Errorf("allNodesGpus.ReserveResourcesGpuOnly: no existe el nodo con nombre %s", nodeName)
	}

	var gpus []*gpuSpec = p.nodes[nodeName]

	if gpuPosition >= len(gpus) || gpuPosition < 0 {
		return fmt.Errorf("allNodesGpus.ReserveResourcesGpuOnly: posición de la gpu fuera de rango %d", gpuPosition)
	}

	gpus[gpuPosition].Lock()
	defer gpus[gpuPosition].Unlock()

	var gpu *gpuSpec = gpus[gpuPosition]

	if gpu.available < gpuUsage {
		return fmt.Errorf("allNodesGpus.ReserveResourcesGpuOnly: no hay suficientes recursos de gpu. gpu libre: %d. gpu demandada", gpu.available, gpuUsage)
	}

	gpu.available -= gpuUsage
	return nil
}
