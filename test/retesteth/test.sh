./dretesteth.sh -t GeneralStateTests/stSystemOperationsTest -- --testpath ./tests --datadir /tests/config -j 10 --clients defaultout
#./dretesteth.sh -t GeneralStateTests/Cancun -- --testpath ./tests --datadir /tests/config --nodes 192.168.3.228:8123 --clients t8ntool-out

#./dretesteth.sh -t GeneralStateTests -- --testpath ./tests --datadir /tests/config --nodes 127.0.0.1:8123 -j 10 --clients t8ntool

#./dretesteth.sh -t GeneralStateTests/stSystemOperationsTest -- --testpath ./tests --datadir /tests/config -j 10 --nodes 192.168.3.228:8123 --clients t8ntool-out

#./dretesteth.sh -t BlockchainTests/ValidBlocks -- --testpath ./tests --datadir /tests/config

#-t GeneralStateTests/Cancun --nodes 192.168.3.228:18123


./dretesteth.sh -t GeneralStateTests/stExample -- --testpath ./tests --datadir /tests/config --clients defaultout
./dretesteth.sh -t GeneralStateTests/stExample -- --testpath ./tests --datadir /tests/config --clients defaultout