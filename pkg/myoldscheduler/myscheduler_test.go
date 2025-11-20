package myscheduler

import (
	"testing"

	// "context"
	v1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
	// clientsetfake "k8s.io/client-go/kubernetes/fake"
	// framework "k8s.io/kubernetes/pkg/scheduler/framework"
	// frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	// metrics "k8s.io/kubernetes/pkg/scheduler/metrics"
	// testf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
)

type ResourceNameandQuantity struct {
	ResourceName v1.ResourceName
	Quantity     resource.Quantity
}

func TestNodeResourcesAllocatable(t *testing.T) {
	// labels0 := map[string]string{
	// 	"beta.kubernetes.io/arch":        "amd64",
	// 	"beta.kubernetes.io/os":          "linux",
	// 	"kubernetes.io/arch":             "amd64",
	// 	"kubernetes.io/hostname":         "kwok-node-0",
	// 	"kubernetes.io/os":               "linux",
	// 	"kubernetes.io/role":             "agent",
	// 	"node-role.kubernetes.io/agent":  "",
	// 	"run.ai/simulated-gpu-node-pool": "pool0",
	// 	"type":                           "kwok",
	// }

	// labels1 := map[string]string{
	// 	"beta.kubernetes.io/arch":        "amd64",
	// 	"beta.kubernetes.io/os":          "linux",
	// 	"kubernetes.io/arch":             "amd64",
	// 	"kubernetes.io/hostname":         "kwok-node-0",
	// 	"kubernetes.io/os":               "linux",
	// 	"kubernetes.io/role":             "agent",
	// 	"node-role.kubernetes.io/agent":  "",
	// 	"run.ai/simulated-gpu-node-pool": "pool1",
	// 	"type":                           "kwok",
	// }

	// labels2 := map[string]string{
	// 	"beta.kubernetes.io/arch":        "amd64",
	// 	"beta.kubernetes.io/os":          "linux",
	// 	"kubernetes.io/arch":             "amd64",
	// 	"kubernetes.io/hostname":         "kwok-node-0",
	// 	"kubernetes.io/os":               "linux",
	// 	"kubernetes.io/role":             "agent",
	// 	"node-role.kubernetes.io/agent":  "",
	// 	"run.ai/simulated-gpu-node-pool": "pool2",
	// 	"type":                           "kwok",
	// }

	// nvidia.com/mig-(slice)g.(mem)gb
	// nvidia.com/mig-1g.6gb  A30

	// var gpu0 ResourceNameandQuantity = ResourceNameandQuantity{ResourceName: v1.ResourceName("nvidia.com/gpu"), Quantity: resource.MustParse("1")}

	// pool0 := map[v1.ResourceName]resource.Quantity{
	// 	v1.ResourceName("nvidia.com/mig-2g.12gb"): resource.MustParse("1"),
	// 	v1.ResourceName("nvidia.com/mig-1g.6gb"):  resource.MustParse("2"),
	// }

	// + CPU y - MEM
	// podRequests0 := v1.ResourceList{
	// 	v1.ResourceCPU:    resource.MustParse("6000m"),
	// 	v1.ResourceMemory: resource.MustParse("3Gi"),
	// 	v1.ResourceName("nvidia.com/mig-2g.12gb"): resource.MustParse("1"),
	// 	v1.ResourceName("nvidia.com/mig-1g.6gb"):  resource.MustParse("2"),
	// }

	// // - CPU y + MEM
	// podRequests1 := v1.ResourceList{
	// 	v1.ResourceCPU:    resource.MustParse("1600m"),
	// 	v1.ResourceMemory: resource.MustParse("10Gi"),
	// 	v1.ResourceName("nvidia.com/mig-2g.12gb"): resource.MustParse("1"),
	// 	v1.ResourceName("nvidia.com/mig-1g.6gb"):  resource.MustParse("2"),
	// }

	// // + CPU + MEM
	// podRequests2 := v1.ResourceList{
	// 	v1.ResourceCPU:    resource.MustParse("10000m"),
	// 	v1.ResourceMemory: resource.MustParse("14Gi"),
	// }

	// fmt.Printf("Limits: %d", podRequests0["nvidia.com/mig-2g.12gb"].AsDec())

	// podRequests0 := map[v1.ResourceName]string{
	// 	v1.ResourceCPU:    "6000m",
	// 	v1.ResourceMemory: "3Gi",
	// 	v1.ResourceName("nvidia.com/mig-2g.12gb"): "1",
	// 	v1.ResourceName("nvidia.com/mig-1g.6gb"):  "2",
	// }

	// var pod0 *v1.Pod = st.MakePod().Name("pod0").Labels(labels0).SchedulerName("scheduler-plugin").Res(podRequests0).Obj()

	// fmt.Printf("Limits: %d", pod0.Spec.Containers[0].Resources.Limits["nvidia.com/mig-2g.12gb"].AsDec())
}
