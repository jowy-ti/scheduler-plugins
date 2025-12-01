package myscheduler

import klog "k8s.io/klog/v2"

// Debugar informacion del nodo en cache
func scanNode(nodeName string) {

	gpuLenght, err := nodeGpus.getLength(nodeName)
	if err != nil {
		klog.V(0).Infof("%v", err)
		return
	}
	klog.V(0).Infof("gpuLenght: %d", gpuLenght)

	for i := 0; gpuLenght > i; i++ {
		gpu, err := nodeGpus.getGeneralGpuResources(nodeName, i)

		if err != nil {
			klog.V(0).Infof("%v", err)
			return
		}

		if gpu.migLength == 0 {
			klog.V(0).Infof("Available: %d", gpu.available)
			klog.V(0).Infof("Memory: %d", gpu.mem)
			klog.V(0).Infof("Fp32: %d", gpu.fp32)
			klog.V(0).Infof("MigLength: %d", gpu.migLength)
		} else {
			for j := 0; gpu.migLength > j; j++ {
				migPartition := gpu.migSlices[j]

				klog.V(0).Infof("Available: %d", migPartition.available)
				klog.V(0).Infof("Memory: %d", migPartition.mem)
				klog.V(0).Infof("Fp32: %d", migPartition.fp32)
				klog.V(0).Infof("Size: %d", migPartition.size)

				j += migPartition.size - 1
			}
		}
	}
}

// Debugar informacion del uso del pod en cache
func scanPodUsage(podName string) {
	resources, err := podsUsage.getPodResources(podName)
	if err != nil {
		klog.V(0).Infof("%v", err)
		return
	}
	klog.V(0).Infof("nodeName: %s", resources.nodeName)
	klog.V(0).Infof("gpuPosition: %d", resources.gpuPosition)
	klog.V(0).Infof("gpuUsage: %d", resources.gpuUsage)
	klog.V(0).Infof("mig: %t", resources.mig)
	klog.V(0).Infof("migPosition: %d", resources.migPosition)
	klog.V(0).Infof("migUsage: %d", resources.migUsage)
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
