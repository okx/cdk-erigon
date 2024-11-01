# How to run
```shell
make build-docker;
cd test; 
# run full test
make run; 
# or run minimum test
make min-run;
# run all with bridge
make all;

cast send -f 0x8f8E2d6cF621f30e9a11309D6A56A876281Fd534  --private-key 0x815405dddb0e2a99b12af775fd2929e526704e1d1aea6a0b4e74dc33e2f7fcd2 --value 0.01ether 0xA6f7A6b2E9B4d41C582D4Aaf907F45321e2Ca847 --legacy --rpc-url http://127.0.0.1:8123
```

# Important
To check the consistency state, turn on the following switch
``` shell
vim config/test.erigon.seq.config.yaml
# modify executor-strict to true
zkevm.executor-strict: true
```

# How to use bridge
```
make all;

http://127.0.0.1:8090/
L1 OKB Token: 0x5FbDB2315678afecb367f032d93F642f64180aa3
L2 WETH Token: 0x5d7AF92af4FF5a35323250D6ee174C23CCBe00EF
L2 admin: 0x8f8E2d6cF621f30e9a11309D6A56A876281Fd534

```

# Get metrics
```
curl http://127.0.0.1:9092/debug/metrics/prometheus
curl http://127.0.0.1:9092/debug/metrics
```
