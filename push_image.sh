# Reemplaza "tu-usuario-dockerhub" por tu usuario real
export REGISTRY=joelgonzalezjimenez

export RELEASE_VERSION=latest

# export GO_BASE_IMAGE=1.25

export PLATFORMS=linux/amd64

# export BUILDER=docker

# Ejecuta la tarea que construye y sube la imagen
make push-images
