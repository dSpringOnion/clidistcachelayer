# Kubernetes Deployment Guide

## Prerequisites

- Kubernetes cluster (v1.20+)
- kubectl configured
- Container image built and available

## Quick Start

### 1. Build and Push Image

```bash
# Build the image
docker build -f docker/Dockerfile.coordinator -t distcache/coordinator:latest .

# Tag for your registry
docker tag distcache/coordinator:latest your-registry/distcache/coordinator:latest

# Push to registry
docker push your-registry/distcache/coordinator:latest
```

### 2. Deploy Coordinator

```bash
# Apply all resources
kubectl apply -f k8s/coordinator-deployment.yaml

# Or deploy step by step
kubectl apply -f k8s/coordinator-deployment.yaml --namespace=distcache
```

### 3. Verify Deployment

```bash
# Check deployment status
kubectl get deployments -n distcache

# Check pod status
kubectl get pods -n distcache

# Check service
kubectl get svc -n distcache

# View logs
kubectl logs -f deployment/coordinator -n distcache
```

## Architecture

The Kubernetes deployment includes:

1. **Namespace**: `distcache` - Isolated namespace for all components
2. **PersistentVolumeClaim**: `coordinator-storage` - 1Gi storage for coordinator state
3. **ConfigMap**: `coordinator-config` - Configuration parameters
4. **Deployment**: `coordinator` - Single replica coordinator instance
5. **Service**: `coordinator` - ClusterIP service on port 50100
6. **Service**: `coordinator-headless` - Headless service for direct pod access

## Configuration

### ConfigMap

Edit configuration in `coordinator-deployment.yaml`:

```yaml
data:
  HEARTBEAT_TIMEOUT: "5000"      # Node heartbeat timeout (ms)
  REPLICATION_FACTOR: "3"        # Cluster replication factor
  VIRTUAL_NODES: "150"           # Virtual nodes per physical node
```

Apply changes:

```bash
kubectl apply -f k8s/coordinator-deployment.yaml
kubectl rollout restart deployment/coordinator -n distcache
```

### Resource Limits

Default resource allocation:

```yaml
resources:
  requests:
    memory: "128Mi"
    cpu: "100m"
  limits:
    memory: "512Mi"
    cpu: "500m"
```

Adjust based on your cluster size and workload.

### Storage

Default PVC size is 1Gi. For larger clusters:

```yaml
spec:
  resources:
    requests:
      storage: 10Gi  # Increase as needed
```

## Scaling

The coordinator runs as a **single replica** (not horizontally scalable):
- Uses `Recreate` strategy to ensure only one instance
- State stored in PersistentVolume
- For HA, consider using StatefulSet with leader election (Phase 3)

## Service Discovery

Cache nodes can discover the coordinator via Kubernetes DNS:

- **Service DNS**: `coordinator.distcache.svc.cluster.local:50100`
- **Headless DNS**: `coordinator-headless.distcache.svc.cluster.local:50100`

## Health Checks

Built-in health probes:

```yaml
livenessProbe:
  tcpSocket:
    port: 50100
  initialDelaySeconds: 10
  periodSeconds: 10

readinessProbe:
  tcpSocket:
    port: 50100
  initialDelaySeconds: 5
  periodSeconds: 5
```

## Monitoring

### Logs

```bash
# Follow logs
kubectl logs -f deployment/coordinator -n distcache

# View recent logs
kubectl logs --tail=100 deployment/coordinator -n distcache

# Logs for specific pod
kubectl logs coordinator-<pod-id> -n distcache
```

### Port Forwarding

Access coordinator from localhost:

```bash
kubectl port-forward -n distcache deployment/coordinator 50100:50100
```

Then test with grpcurl:

```bash
grpcurl -plaintext localhost:50100 list
grpcurl -plaintext localhost:50100 distcache.v1.CoordinatorService/GetClusterStatus
```

## Backup and Restore

### Backup

```bash
# Backup coordinator data
POD=$(kubectl get pod -n distcache -l app=coordinator -o jsonpath='{.items[0].metadata.name}')
kubectl cp -n distcache $POD:/data/coordinator_data.json ./backup-coordinator-data.json
```

### Restore

```bash
# Stop coordinator
kubectl scale deployment/coordinator -n distcache --replicas=0

# Copy backup
POD=$(kubectl get pod -n distcache -l app=coordinator -o jsonpath='{.items[0].metadata.name}')
kubectl cp -n distcache ./backup-coordinator-data.json $POD:/data/coordinator_data.json

# Restart coordinator
kubectl scale deployment/coordinator -n distcache --replicas=1
```

## Troubleshooting

### Pod not starting

```bash
kubectl describe pod -n distcache -l app=coordinator
kubectl logs -n distcache -l app=coordinator --previous
```

### Service not accessible

```bash
kubectl get endpoints -n distcache coordinator
kubectl get svc -n distcache coordinator
```

### Storage issues

```bash
kubectl describe pvc -n distcache coordinator-storage
kubectl get pv
```

## Security Best Practices

1. **Network Policies**: Restrict coordinator access
2. **RBAC**: Limit service account permissions
3. **Pod Security**: Use security contexts
4. **Secrets**: Store sensitive config in Secrets, not ConfigMaps
5. **TLS**: Enable TLS for gRPC (Phase 3)

## Clean Up

```bash
# Delete all resources
kubectl delete -f k8s/coordinator-deployment.yaml

# Or delete namespace
kubectl delete namespace distcache
```

## Production Considerations

For production deployments:

1. **High Availability**: Implement leader election (Phase 3)
2. **Monitoring**: Integrate Prometheus metrics
3. **Alerting**: Set up alerts for coordinator failures
4. **Backup**: Automate coordinator state backups
5. **Resource Tuning**: Adjust based on cluster size
6. **TLS**: Enable secure communication
7. **Authentication**: Implement access control

## Next Steps

- Deploy cache nodes (distcache_server)
- Configure nodes to connect to coordinator
- Set up monitoring and alerting
- Enable TLS and authentication
- Implement automated backups
