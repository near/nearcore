import time
import rpc

nonce = 0
rpc = rpc.NearRPC("http://127.0.0.1:3030", 1)

while True:
    print(rpc.call_function(2, "run_test", []))
    time.sleep(1)
    print(rpc.send_money(2, 1))
    time.sleep(1)
