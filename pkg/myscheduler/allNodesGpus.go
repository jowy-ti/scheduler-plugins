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

// Metodos

// Constructora
func newAllNodesGpus() *allNodesGpus {
	return &allNodesGpus{
		nodes: make(map[string][]*gpuSpec),
	}
}

func (p *allNodesGpus) getMig(nodeName string, gpuPosition int) (bool, error) {

	p.RLock()
	defer p.RUnlock()

	gpu, err := p.getGpu(nodeName, gpuPosition)

	if err != nil {
		return false, err
	}

	gpu.RLock()
	defer gpu.RUnlock()

	return gpu.mig, nil
}

func (p *allNodesGpus) getGpuAvailable(nodeName string, gpuPosition int) (int, error) {
	p.RLock()
	defer p.RUnlock()

	gpu, err := p.getGpu(nodeName, gpuPosition)

	if err != nil {
		return 0, err
	}

	gpu.RLock()
	defer gpu.RUnlock()

	return gpu.available, nil
}

func (p *allNodesGpus) getMigInfo(nodeName string, gpuPosition int, migPosition int) (available int, size int, mem int64, err error) {
	p.RLock()
	defer p.RUnlock()

	gpu, err := p.getGpu(nodeName, gpuPosition)

	if err != nil {
		return 0, 0, 0, err
	}

	gpu.RLock()
	defer gpu.RUnlock()

	migSlice, err := gpu.getMigSlice(migPosition)

	if err != nil {
		return 0, 0, 0, err
	}

	return migSlice.available, migSlice.size, migSlice.mem, nil
}

// Metodos para podsGpuUsage

func (p *allNodesGpus) cleanResourcesGpuOnly(nodeName string, gpuPosition int, gpuUsage int) error {

	p.RLock()
	defer p.RUnlock()

	gpu, err := p.getGpu(nodeName, gpuPosition)

	if err != nil {
		return err
	}

	gpu.Lock()
	defer gpu.Unlock()

	if gpu.available+gpuUsage > maxAvailabilityGpu {
		return fmt.Errorf("allNodesGpus.cleanResourcesGpuOnly: incoherencia entre gpu libre y la que se tiene que liberar. gpu libre: %d. gpu a liberar %d", gpu.available, gpuUsage)
	}

	gpu.available += gpuUsage
	return nil
}

func (p *allNodesGpus) reserveResourcesGpuOnly(nodeName string, gpuPosition int, gpuUsage int) error {

	p.RLock()
	defer p.RUnlock()

	gpu, err := p.getGpu(nodeName, gpuPosition)

	if err != nil {
		return err
	}

	gpu.Lock()
	defer gpu.Unlock()

	if gpu.available < gpuUsage {
		return fmt.Errorf("allNodesGpus.reserveResourcesGpuOnly: no hay suficientes recursos de gpu. gpu libre: %d. gpu demandada", gpu.available, gpuUsage)
	}

	gpu.available -= gpuUsage
	return nil
}

func (p *allNodesGpus) cleanResourcesMigOnly(nodeName string, gpuPosition int, migPosition int, migUsage int) error {

	p.RLock()
	defer p.RUnlock()

	gpu, err := p.getGpu(nodeName, gpuPosition)

	if err != nil {
		return err
	}

	gpu.RLock()
	defer gpu.RUnlock()

	migSlice, err := gpu.getMigSlice(migPosition)

	if err != nil {
		return err
	}

	migSlice.Lock()
	defer migSlice.Unlock()

	if migSlice.available+migUsage > maxAvailabilityGpu {
		return fmt.Errorf("allNodesGpus.cleanResourcesMigOnly: incoherencia entre MIG libre y la que se tiene que liberar. Partición MIG libre: %d. partición MIG a liberar", migSlice.available, migUsage)
	}

	migSlice.available += migUsage
	return nil
}

func (p *allNodesGpus) reserveResourcesMigOnly(nodeName string, gpuPosition int, migPosition int, migUsage int) error {

	p.RLock()
	defer p.RUnlock()

	gpu, err := p.getGpu(nodeName, gpuPosition)

	if err != nil {
		return err
	}

	gpu.RLock()
	defer gpu.RUnlock()

	migSlice, err := gpu.getMigSlice(migPosition)

	if err != nil {
		return err
	}

	migSlice.Lock()
	defer migSlice.Unlock()

	if migSlice.available < migUsage {
		return fmt.Errorf("allNodesGpus.reserveResourcesMigOnly: no hay suficientes recursos en la partición MIG. Partición MIG libre: %d. partición MIG demandada", migSlice.available, migUsage)
	}

	migSlice.available -= migUsage
	return nil
}

// Metodos privados no protegido por Mutex!!

// Lectura
func (p *allNodesGpus) getGpu(nodeName string, gpuPosition int) (*gpuSpec, error) {

	if _, exists := p.nodes[nodeName]; !exists {
		return nil, fmt.Errorf("allNodesGpus.getGpu: no existe el nodo con nombre %s", nodeName)
	}

	var gpus []*gpuSpec = p.nodes[nodeName]

	if gpuPosition >= len(gpus) || gpuPosition < 0 {
		return nil, fmt.Errorf("allNodesGpus.getGpu: posición de la gpu fuera de rango %d", gpuPosition)
	}
	return gpus[gpuPosition], nil
}

// Metodos gpuSpec
func (g *gpuSpec) getMigSlice(migPosition int) (*migPartition, error) {

	var migSlices []*migPartition = g.migSlices

	if migPosition >= len(migSlices) || migPosition < 0 {
		return nil, fmt.Errorf("allNodesGpus.getMigSlice: posición de MIG fuera de rango %d", migPosition)
	}

	return migSlices[migPosition], nil
}
