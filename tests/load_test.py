import asyncio
import json
import time
import sys

async def run_load_test(num_requests=100):
    print(f"Starting MCP Server...")
    # Command to run MCP server
    cmd = [".venv\\\\Scripts\\\\prometheus-mcp-server.exe"]
    
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # 1. Initialize MCP
    init_req = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "load-tester", "version": "1.0.0"}
        }
    }
    
    process.stdin.write((json.dumps(init_req) + "\n").encode())
    await process.stdin.drain()
    
    line = await process.stdout.readline()
    
    init_notif = {
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    }
    process.stdin.write((json.dumps(init_notif) + "\n").encode())
    await process.stdin.drain()

    print(f"Server initialized. Bombarding with {num_requests} concurrent tool calls...")
    start_time = time.time()
    
    # 2. Pipeline requests (send all without waiting for response)
    for i in range(2, num_requests + 2):
        # We will test `docker_ps` as it does actual IO with Docker daemon and regex matching
        req = {
            "jsonrpc": "2.0",
            "id": i,
            "method": "tools/call",
            "params": {
                "name": "execute_query",
                "arguments": {"query": "up"}
            }
        }
        process.stdin.write((json.dumps(req) + "\n").encode())
    
    await process.stdin.drain()
    
    # 3. Read responses
    responses = 0
    errors = 0
    while responses + errors < num_requests:
        try:
            line = await asyncio.wait_for(process.stdout.readline(), timeout=15.0)
            if not line:
                break
            resp = json.loads(line.decode().strip())
            if "id" in resp and resp["id"] >= 2:
                if "error" in resp or ("result" in resp and getattr(resp["result"], "isError", False)):
                    errors += 1
                else:
                    responses += 1
                if (responses + errors) % 10 == 0:
                    print(f"Progress: {responses + errors} / {num_requests} completed")
        except asyncio.TimeoutError:
            print("Timeout reading response!")
            break
        except Exception as e:
            print("Parse error:", e)

    end_time = time.time()
    duration = end_time - start_time
    
    print("-" * 30)
    print("LOAD TEST RESULTS")
    print("-" * 30)
    print(f"Total Requests Sent : {num_requests}")
    print(f"Successful Replies  : {responses}")
    print(f"Errors/Timeouts     : {errors}")
    print(f"Total Time Taken    : {duration:.4f} seconds")
    print(f"Throughput          : {(responses+errors) / duration:.2f} requests/second")
    print("-" * 30)
    
    process.terminate()

if __name__ == "__main__":
    asyncio.run(run_load_test(100))
