
# Algo Trade

## Basic Docker setup

```docker network create algoTrade```

```docker volume create algoTrade```

## Redis setup

``` docker run -d --network algoTrade --name algo-trade-redis -v algoTrade:/data redis```

## Setup repo docker

```docker build -t algo-trade-node .```

```docker run -d --network algoTrade --name algo-trade-node -p 3000:3000 algo-trade-node```

## n8n local setup

```docker volume create n8n_data```

```docker run -d --network algoTrade --name algo-trade-n8n -p 5678:5678 -v n8n_data:/data docker.n8n.io/n8nio/n8n```
