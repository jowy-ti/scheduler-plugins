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

func newPodsGpuUsage() *podsGpuUsage {
	return &podsGpuUsage{
		pods: make(map[string]*podResources),
	}
}

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

func (p *podsGpuUsage) addPodResourcesGpuOnly(podName string, nodeName string, gpuPosition int, gpuUsage int) error {

	var resources *podResources = &podResources{
		nodeName:    nodeName,
		mig:         false,
		gpuPosition: gpuPosition,
		gpuUsage:    gpuUsage,
	}

	if err := p.addPodResources(podName, resources); err != nil {
		return err
	}

	if err := nodeGpus.ReserveResourcesGpuOnly(nodeName, gpuPosition, gpuUsage); err != nil {
		deleteErr := p.deletePodResources(podName)

		if deleteErr != nil {
			return fmt.Errorf("podsGpuUsage.addPodResourcesGpuOnly: Error al hacer la reserva de recursos y error para limpiar la reserva para el pod %s", podName)
		}
		return err
	}

	return nil
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
