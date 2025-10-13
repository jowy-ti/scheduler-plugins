Instalar dependecias:

- make
- docker (toda la instalación)

Importante:
scheduler-plugins/image_push.sh 
scheduler-plugins/manifests/install/charts/as-a-second-scheduler/values.yaml

Despliegue del contenedor con el plugin con helm chart:

cd scheduler-plugins/manifests/install/charts
helm install --repo https://scheduler-plugins.sigs.k8s.io scheduler-plugins scheduler-plugins --namespace plugins --create-namespace -f ~/go/src/sigs.k8s.io/scheduler-plugins/manifests/install/charts/as-a-second-scheduler/myvalues.yaml
values.yaml path:  scheduler-plugins/manifests/charts/values.yaml
kubectl apply -f ~/go/src/sigs.k8s.io/scheduler-plugins/pkg/myscheduler/test.yaml


Cambiar el main.go:

Añadir el path del directorio donde está el scheduler personalizado en "import"

Añadir el nombre y el inicializador en "app.NewSchedulerCommand()". Ej: app.WithPlugin(myscheduler.Name, myscheduler.New)



Plugin:

El plugin tiene que tener siempre estás estructuras y funciones:

- Struct principal, el corazón del plugin con los métodos, el cual kube-scheduler deberá inicializar
type MyScheduler struct {
	handle framework.Handle
}

- Función de creación, para que kubescheduler pueda instanciar el plugin (struct)
func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error)

- Función que devuelve le nombre del struct
func (m *MyScheduler) Name() string



Warning:
la imagen para compilar el plugin del scheduler tiene que ser bookworm (debian)