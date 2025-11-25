package myscheduler

import (
	"fmt"
	"sync"
)

// Mapa del uso de recursos de los pods
type podsGpuUsage struct {
	sync.RWMutex
	pods map[string]*podResources
}

// Nodo asignado al pod y info de utilización de gpu
type podResources struct {
	gpuPosition int
	migPosition int
	migUsage    int
	gpuUsage    int
	nodeName    string
	mig         bool
}

// Constructora
func newPodsGpuUsage() *podsGpuUsage {
	return &podsGpuUsage{
		pods: make(map[string]*podResources),
	}
}

// Setters
func (p *podsGpuUsage) setPodResourcesGpuOnly(podName string, nodeName string, gpuPosition int, gpuUsage int) error {

	var resources *podResources = &podResources{
		nodeName:    nodeName,
		mig:         false,
		gpuPosition: gpuPosition,
		gpuUsage:    gpuUsage,
	}

	if err := p.addPodResources(podName, resources); err != nil {
		return err
	}

	if err := nodeGpus.reserveResourcesGpuOnly(nodeName, gpuPosition, gpuUsage); err != nil {
		deleteErr := p.deletePodResources(podName)

		if deleteErr != nil {
			return fmt.Errorf("podsGpuUsage.setPodResourcesGpuOnly: Error en la reserva de recursos: %w. Error en la limpieza de la reserva para el pod %w", err, deleteErr)
		}
		return err
	}
	return nil
}

func (p *podsGpuUsage) setPodResourcesMigOnly(podName string, nodeName string, gpuPosition int, migPosition int, migUsage int) error {

	var resources *podResources = &podResources{
		nodeName:    nodeName,
		mig:         true,
		gpuPosition: gpuPosition,
		migPosition: migPosition,
		migUsage:    migUsage,
	}

	if err := p.addPodResources(podName, resources); err != nil {
		return err
	}

	if err := nodeGpus.reserveResourcesMigOnly(nodeName, gpuPosition, migPosition, migUsage); err != nil {
		deleteErr := p.deletePodResources(podName)

		if deleteErr != nil {
			return fmt.Errorf("podsGpuUsage.setPodResourcesMigOnly: Error en la reserva de recursos: %w. Error en la limpieza de la reserva para el pod %w", err, deleteErr)
		}
		return err
	}

	return nil
}

// Cleaners
func (p *podsGpuUsage) cleanPodResources(podName string) error {

	pResources, err := p.getPodResources(podName)

	if err != nil {
		return err
	}

	if !pResources.mig {
		err = nodeGpus.cleanResourcesGpuOnly(pResources.nodeName, pResources.gpuPosition, pResources.gpuUsage)

		if err != nil {
			return err
		}

	} else {
		err = nodeGpus.cleanResourcesMigOnly(pResources.nodeName, pResources.gpuPosition, pResources.migPosition, pResources.migUsage)

		if err != nil {
			return err
		}
	}

	if err = p.deletePodResources(podName); err != nil {
		return err
	}
	return nil
}

// Geters
func (p *podsGpuUsage) getPodResources(podName string) (resources *podResources, err error) {

	p.RLock()
	defer p.RUnlock()
	podGpuUsage, ok := p.pods[podName]

	if !ok {
		return nil, fmt.Errorf("podsGpuUsage.getPodResources: no existe el pod con nombre %s", podName)
	}

	var pResources *podResources = &podResources{
		nodeName:    podGpuUsage.nodeName,
		mig:         podGpuUsage.mig,
		gpuPosition: podGpuUsage.gpuPosition,
		gpuUsage:    podGpuUsage.gpuUsage,
		migPosition: podGpuUsage.migPosition,
		migUsage:    podGpuUsage.migUsage,
	}
	return pResources, nil
}

// Metodos privados
func (p *podsGpuUsage) addPodResources(podName string, resources *podResources) error {
	p.Lock()
	defer p.Unlock()

	if _, exists := p.pods[podName]; exists {
		return fmt.Errorf("podsGpuUsage.addPodResources: ya existe una reserva de recursos para el pod %s", podName)
	}

	p.pods[podName] = resources

	return nil
}

func (p *podsGpuUsage) deletePodResources(podName string) error {
	p.Lock()
	defer p.Unlock()

	if _, exists := p.pods[podName]; !exists {
		return fmt.Errorf("podsGpuUsage.deletePodResources: no existe una reserva de recursos para el pod %s", podName)
	}

	delete(p.pods, podName)

	return nil
}

// metodos
