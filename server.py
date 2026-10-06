import asyncio
import json
import websockets

players = {}
doors = {}
tv_state = {'url': ''}

async def broadcast(message, exclude=None):
    if players:
        tasks = []
        for pid, p in players.items():
            if pid != exclude and p['ws']:
                tasks.append(asyncio.create_task(p['ws'].send(json.dumps(message))))
        if tasks:
            await asyncio.wait(tasks)

async def handler(websocket):
    player_id = str(id(websocket))
    players[player_id] = { 'ws': websocket, 'state': {} }

    try:
        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get('type')

            if msg_type == 'join':
                init_data = { 'type': 'init', 'id': player_id, 'players': { k: v['state'] for k,v in players.items() if k != player_id }, 'doors': doors, 'tv_url': tv_state['url'] }
                await websocket.send(json.dumps(init_data))
                
                players[player_id]['state'] = data.get('state', {})
                players[player_id]['state']['id'] = player_id
                
                await broadcast({ 'type': 'player_joined', 'player': players[player_id]['state'] }, exclude=player_id)

            elif msg_type == 'move':
                players[player_id]['state'] = data.get('state', {})
                players[player_id]['state']['id'] = player_id
                await broadcast({ 'type': 'player_moved', 'player': players[player_id]['state'] }, exclude=player_id)

            elif msg_type == 'door_request':
                door_id = data.get('doorId')
                door = doors.get(door_id, {'state': 'closed', 'owner': None})
                if door['owner'] is None or door['owner'] == player_id:
                    door['owner'] = player_id
                    door['state'] = data.get('newState')
                    doors[door_id] = door
                    await broadcast({ 'type': 'door_update', 'doorId': door_id, 'state': door['state'], 'owner': player_id })
                else:
                    await websocket.send(json.dumps({ 'type': 'door_rejected', 'doorId': door_id }))

            elif msg_type == 'door_release':
                door_id = data.get('doorId')
                door = doors.get(door_id, {'state': 'open', 'owner': None})
                if door['owner'] == player_id:
                    door['owner'] = None
                    door['state'] = data.get('finalState')
                    doors[door_id] = door
                    await broadcast({ 'type': 'door_update', 'doorId': door_id, 'state': door['state'], 'owner': None })

            elif msg_type == 'tv_sync':
                tv_state['url'] = data.get('url')
                await broadcast({ 'type': 'tv_update', 'url': tv_state['url'] })

            elif msg_type == 'signal':
                target = data.get('target')
                if target in players and players[target]['ws']:
                    await players[target]['ws'].send(json.dumps({
                        'type': 'signal',
                        'sender': player_id,
                        'signalData': data.get('signalData')
                    }))

    finally:
        del players[player_id]
        await broadcast({ 'type': 'player_left', 'id': player_id })

import os

async def main():
    port = int(os.environ.get("PORT", 8080))
    print(f"Zepherspace WebSocket Server running on port {port}")
    async with websockets.serve(handler, "0.0.0.0", port):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
