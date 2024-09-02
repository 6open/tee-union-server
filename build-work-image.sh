# docker build -f docker/Dockerfile -t 192.168.50.32/mpc-dev/tee-sql-service:1.0.0 .
# docker push 192.168.50.32/mpc-dev/tee-sql-service:1.0.0

# docker build -f docker/predeal-Dockerfile -t 192.168.50.32/mpc-dev/tee-sql-service-biaopin:1.0.0 .
# docker push 192.168.50.32/mpc-dev/tee-sql-service-biaopin:1.0.0

# 预处理标品分支
# docker build -f docker/predeal-Dockerfile -t 192.168.50.32/mpc-dev/pre-deal-bp:1.0.0 .
# docker push 192.168.50.32/mpc-dev/pre-deal-bp:1.0.0

# 任务处理标品分支
docker build -f docker/task-Dockerfile -t 192.168.50.32/mpc-dev/tee-sql-service-bp:1.0.0 .
docker push 192.168.50.32/mpc-dev/tee-sql-service-bp:1.0.0

# 预处理1k分支
# docker build -f docker/predeal-Dockerfile -t 192.168.50.32/mpc-dev/pre-deal-1k:1.0.0 .
# docker push  192.168.50.32/mpc-dev/pre-deal-1k:1.0.0

# 任务处理1k分支
# docker build -f docker/task-1k-Dockerfile -t 192.168.50.32/mpc-dev/tee-sql-service-1k:1.0.0 .
# docker push 192.168.50.32/mpc-dev/tee-sql-service-1k:1.0.0