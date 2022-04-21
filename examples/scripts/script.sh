#!/bin/bash

wget https://go.dev/dl/go1.17.8.linux-amd64.tar.gz
rm -rf /usr/local/go && tar -C /usr/local -xzf go1.17.8.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
go version


wget https://dist.ipfs.io/go-ipfs/v0.11.0/go-ipfs_v0.11.0_linux-amd64.tar.gz
tar -xvzf go-ipfs_v0.11.0_linux-amd64.tar.gz
cd go-ipfs
source install.sh
ipfs version

sudo mkdir .ipfs
sudo chmod 777 .ipfs
setenv IPFS_PATH /data/.ipfs
ipfs init

echo "/key/swarm/psk/1.0.0/\n/base16/\n`tr -dc 'a-f0-9' < /dev/urandom | head -c64`" > /data/.ipfs/swarm.key
# copy swarm to all nodes

ipfs bootstrap rm --all
ipfs config show | grep "PeerID"

hostname -I
ipfs bootstrap add /ip4/155.98.38.79/tcp/4001/ipfs/12D3KooWRnRc21LG1LVNd2vWuoqoR1oAEpBSaKi8Hb4D7rUkU4YQ

	
ulimit -n 1000000



ipfs daemon &

for cid in $(cat /users/cs6675g7/o.t); do ipfs cat $cid; done

kill -9 $(pidof ipfs)



sudo chown cs6675g7 -R go-ipfs-mapreduce/

go run mr_run.go /users/cs6675g7/blog3.csv /users/cs6675g7/o.t 3000


go run mr_run.go 2 dummy output /users/cs6675g7/o.t 200000