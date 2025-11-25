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
	mem       int
	migSlices []*migSlice
	mig       bool
}

// Informacion de la particion de MIG
type migSlice struct {
	sync.RWMutex
	available int
	size      int
	mem       int
}

// Constructora
func newAllNodesGpus() *allNodesGpus {
	return &allNodesGpus{
		nodes: make(map[string][]*gpuSpec),
	}
}

// Setters
func (p *allNodesGpus) setAllNodesGpus(gpus []*gpuSpec, nodeName string) error {
	if gpus == nil {
		return fmt.Errorf("allNodesGpus.setAllNodesGpus: no se puede construir allNodesGpus si []*gpuSpec es nil")
	}
	p.Lock()
	defer p.Unlock()
	p.nodes[nodeName] = gpus
	return nil
}

// Getters

func (p *allNodesGpus) getLength(nodeName string) (int, error) {
	p.RLock()
	defer p.RUnlock()

	gpus, exists := p.nodes[nodeName]
	if !exists {
		return 0, fmt.Errorf("allNodesGpus.getLength: no existe el nodo con nombre %s", nodeName)
	}
	// var length int = len(gpus)
	// if length == 0 {
	// 	return 0, fmt.Errorf("allNodesGpus.getLength: no hay gpus en nodo %s", nodeName)
	// }
	return len(gpus), nil
}

func (p *allNodesGpus) getGeneralGpuResources(nodeName string, gpuPosition int) (available int, mem int, mig bool, err error) {
	p.RLock()
	defer p.RUnlock()

	gpu, err := p.getGpu(nodeName, gpuPosition)

	if err != nil {
		return 0, 0, false, err
	}

	gpu.RLock()
	defer gpu.RUnlock()

	return gpu.available, gpu.mem, gpu.mig, nil
}

func (p *allNodesGpus) getMigResources(nodeName string, gpuPosition int, migPosition int) (available int, size int, mem int, err error) {
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
		return fmt.Errorf("allNodesGpus.reserveResourcesGpuOnly: no hay suficientes recursos de gpu. gpu libre: %d. gpu demandada %d", gpu.available, gpuUsage)
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
		return fmt.Errorf("allNodesGpus.cleanResourcesMigOnly: incoherencia entre MIG libre y la que se tiene que liberar. Partición MIG libre: %d. partición MIG a liberar %d", migSlice.available, migUsage)
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
		return fmt.Errorf("allNodesGpus.reserveResourcesMigOnly: no hay suficientes recursos en la partición MIG. Partición MIG libre: %d. partición MIG demandada %d", migSlice.available, migUsage)
	}

	migSlice.available -= migUsage
	return nil
}

// Metodos privados no protegidos por Mutex!!
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
func newGpuSpec() *gpuSpec {
	return &gpuSpec{}
}

// Setters
func (g *gpuSpec) setGpuSpecGpuOnly(available int, mem int) error {
	if available > maxAvailabilityGpu || available < 0 {
		return fmt.Errorf("gpuSpec.setGpuSpecGpuOnly: el valor de available debe de estar en el rango [0,maxAvailabilityGpu], su valor es %d", available)
	}
	g.available = available
	g.mem = mem
	g.mig = false
	return nil
}

func (g *gpuSpec) setGpuSpecMigOnly(migPartition []*migSlice, mem int) error {
	if migPartition == nil {
		return fmt.Errorf("gpuSpec.setGpuSpecMigOnly: no se puede crear un *gpuSpec con mig y migPartition nil")
	}
	g.mem = mem
	g.mig = true
	g.migSlices = migPartition
	return nil
}

// Metodos privados no protegido por Mutex!!
func (g *gpuSpec) getMigSlice(migPosition int) (*migSlice, error) {

	var migSlices []*migSlice = g.migSlices

	if migPosition >= len(migSlices) || migPosition < 0 {
		return nil, fmt.Errorf("gpuSpec.getMigSlice: posición de MIG fuera de rango %d", migPosition)
	}

	return migSlices[migPosition], nil
}

// Metodos migSlice

func newMigSlice() *migSlice {
	return &migSlice{}
}

// Setters
func (m *migSlice) setInfoMigSlice(available int, size int, mem int) error {
	if available > maxAvailabilityGpu || available < 0 {
		return fmt.Errorf("migSlice.setInfoMigSlice: el valor de available debe de estar en el rango [0,maxAvailabilityGpu], su valor es %d", available)
	}
	m.available = available
	m.size = size
	m.mem = mem
	return nil
}
