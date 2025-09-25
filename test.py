# tests/ws_probe.py
import asyncio
import json
import websockets  # pip install websockets

async def main():
    uri = "ws://localhost:8000/ws/scheduler"
    async with websockets.connect(uri) as ws:
        print("Connected")
        try:
            while True:
                raw = await ws.recv()
                payload = json.loads(raw)
                print(json.dumps(payload, indent=2, ensure_ascii=False))
        except websockets.ConnectionClosed as exc:
            print("Connection closed", exc)

if __name__ == "__main__":
    asyncio.run(main())