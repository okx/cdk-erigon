wget -P ~/ http://retesteth.ethdevops.io/dretesteth.tar;
docker image load --input ~/dretesteth.tar
wget https://raw.githubusercontent.com/ethereum/retesteth/master/dretesteth.sh;
chmod +x dretesteth.sh
git clone --branch develop https://github.com/ethereum/tests.git
./dretesteth.sh -t GeneralStateTests/stExample -- --testpath ./tests --datadir /tests/config