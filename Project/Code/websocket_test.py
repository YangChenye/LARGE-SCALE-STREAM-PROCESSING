import asyncio
import websockets


async def generator(websocket, path):
    msg = await websocket.recv()
    print(msg)

    response = 'KEKW'
    await websocket.send(response)

start_server = websockets.serve(generator, "localhost", 12302)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
