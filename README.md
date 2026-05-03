1) Старт кластера:
```bash
minikube start
```

2) Збілдити образи у Docker Minikube:
```bash
eval "$(minikube docker-env)"
docker build -t logging-service:latest ./logging_service
docker build -t counter-service:latest ./counter_service
docker build -t facade-service:latest ./facade_service
```

3) Деплой у Kubernetes:
```bash
kubectl apply -k ./k8s
kubectl -n apz-task5 get pods -w
```

4) Доступ до `facade-service`:
```bash
kubectl -n apz-task5 port-forward svc/facade-service 8000:8000
curl http://localhost:8000/health
```

5) Перевірка Service Discovery (ендпоінти)
```bash
kubectl -n apz-task5 get endpoints logging-service -o wide
kubectl -n apz-task5 get endpoints counter-service -o wide
```

6) Перформанс-тести
Перед запуском тестів тримай активним port-forward на `facade-service`:
```bash
kubectl -n apz-task5 port-forward svc/facade-service 8000:8000
python3 tests.py
```

7) Перезапуск (без видалення даних)
```bash
kubectl -n apz-task5 rollout restart deploy/logging-service deploy/counter-service deploy/facade-service deploy/kafka deploy/postgres
kubectl -n apz-task5 rollout restart statefulset/hazelcast
```

8) Знести весь стек:
```bash
kubectl delete -k ./k8s
```

9) Повний ресет з видаленням даних (Postgres PVC):
```bash
kubectl delete -k ./k8s
kubectl -n apz-task5 delete pvc --all
kubectl delete ns apz-task5
```