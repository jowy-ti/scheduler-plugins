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
	nodeName    string
	mig         bool
	gpuPosition int
	migPosition int
	migUsage    int
	gpuUsage    int
}

// Metodos del struct podsGpuUsage

// Constructora
func newPodsGpuUsage() *podsGpuUsage {
	return &podsGpuUsage{
		pods: make(map[string]*podResources),
	}
}

func (p *podsGpuUsage) getNodeName(podName string) (string, error) {

	p.RLock()
	defer p.RUnlock()
	podGpuUsage, ok := p.pods[podName]

	if !ok {
		return "", fmt.Errorf("podsGpuUsage.getNodeName: no existe el pod con nombre %s", podName)
	}

	return podGpuUsage.nodeName, nil
}

func (p *podsGpuUsage) getMig(podName string) (bool, error) {

	p.RLock()
	defer p.RUnlock()
	podGpuUsage, ok := p.pods[podName]

	if !ok {
		return false, fmt.Errorf("podsGpuUsage.getMig: no existe el pod con nombre %s", podName)
	}

	return podGpuUsage.mig, nil
}

func (p *podsGpuUsage) getGpuUsage(podName string) (gpuPosition int, gpuUsage int, err error) {

	p.RLock()
	defer p.RUnlock()
	podGpuUsage, ok := p.pods[podName]

	if !ok {
		return 0, 0, fmt.Errorf("podsGpuUsage.getGpuUsage: no existe el pod con nombre %s", podName)
	}

	return podGpuUsage.gpuPosition, podGpuUsage.gpuUsage, nil
}

func (p *podsGpuUsage) getMigUsage(podName string) (gpuPosition int, migPosition int, migUsage int, err error) {

	p.RLock()
	defer p.RUnlock()
	podGpuUsage, ok := p.pods[podName]

	if !ok {
		return 0, 0, 0, fmt.Errorf("podsGpuUsage.getMigUsage: no existe el pod con nombre %s", podName)
	}

	return podGpuUsage.gpuPosition, podGpuUsage.migPosition, podGpuUsage.migUsage, nil
}

func (p *podsGpuUsage) setPodResourcesGpuOnly(podName string, nodeName string, gpuPosition int, gpuUsage int) error {

	var resources *podResources = &podResources{
		nodeName:    nodeName,
		mig:         false,
		gpuPosition: gpuPosition,
		gpuUsage:    gpuUsage,
	}

	if err := p.setPodResources(podName, resources); err != nil {
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

	if err := p.setPodResources(podName, resources); err != nil {
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

func (p *podsGpuUsage) cleanPodResources(podName string) error {

	nodeName, err := p.getNodeName(podName)

	if err != nil {
		return err
	}
	mig, err := p.getMig(podName)

	if err != nil {
		return err
	}

	if !mig {
		gpuPosition, gpuUsage, err := p.getGpuUsage(podName)

		if err != nil {
			return err
		}
		err = nodeGpus.cleanResourcesGpuOnly(nodeName, gpuPosition, gpuUsage)

		if err != nil {
			return err
		}

	} else {

		gpuPosition, migPosition, migUsage, err := p.getMigUsage(podName)

		if err != nil {
			return err
		}
		err = nodeGpus.cleanResourcesMigOnly(nodeName, gpuPosition, migPosition, migUsage)

		if err != nil {
			return err
		}
	}

	if err = p.deletePodResources(podName); err != nil {
		return err
	}
	return nil
}

// Metodos privados

func (p *podsGpuUsage) setPodResources(podName string, resources *podResources) error {
	p.Lock()
	defer p.Unlock()

	if _, exists := p.pods[podName]; exists {
		return fmt.Errorf("podsGpuUsage.setPodResources: ya existe una reserva de recursos para el pod %s", podName)
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
