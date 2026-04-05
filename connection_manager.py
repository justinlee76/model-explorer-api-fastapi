from asyncio import Lock
from collections import defaultdict
from collections.abc import Iterable
import logging
from typing import Any
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger(__name__)

class ConnectionManager[T]:
    def __init__(self) -> None:
        self.lock = Lock()
        self.subscriptions_by_socket: dict[WebSocket, set[T]] = {}
        self.sockets_by_subscription: dict[T, set[WebSocket]] = defaultdict(set)
    
    async def connect(self, socket: WebSocket) -> None:
        logger.debug('Connecting')

        await socket.accept()
        async with self.lock:
            self.subscriptions_by_socket[socket] = set()
    
    def _remove_socket_for_key(self, socket: WebSocket, key: T) -> None:
        if key not in self.sockets_by_subscription:
            logger.warning('%s not found in sockets_by_subscription', key)
            return
        
        sockets = self.sockets_by_subscription[key]
        sockets.discard(socket)
        if not sockets:
            del self.sockets_by_subscription[key]

    async def disconnect(self, sockets: Iterable[WebSocket]) -> None:
        logger.debug('Disconnecting')
        
        async with self.lock:
            for socket in sockets:
                if socket not in self.subscriptions_by_socket:
                    logger.warning('Socket not found in subscriptions_by_socket when attempting to disconnect')
                    continue
                
                keys = self.subscriptions_by_socket.pop(socket)

                for key in keys:
                    self._remove_socket_for_key(socket, key)

    async def disconnect_one(self, socket: WebSocket) -> None:
        await self.disconnect((socket,))
    
    async def subscribe(self, socket: WebSocket, keys: Iterable[T]) -> None:
        logger.debug('Subscribing to %s', keys)

        async with self.lock:
            for key in keys:
                if socket not in self.subscriptions_by_socket:
                    logger.warning('Socket not found in subscriptions_by_socket when attempting to subscribe to %s', key)
                    continue
                
                self.subscriptions_by_socket[socket].add(key)
                self.sockets_by_subscription[key].add(socket)

    async def subscribe_one(self, socket: WebSocket, key: T) -> None:
        await self.subscribe(socket, (key,))
    
    async def unsubscribe(self, socket: WebSocket, keys: Iterable[T]) -> None:
        logger.debug('Unsubscribing from %s', keys)

        async with self.lock:
            for key in keys:
                if socket not in self.subscriptions_by_socket:
                    logger.warning('Socket not found in subscriptions_by_socket when attempting to unsubscribe from %s', key)
                    continue
                    
                self.subscriptions_by_socket[socket].discard(key)
                
                self._remove_socket_for_key(socket, key)
    
    async def unsubscribe_one(self, socket: WebSocket, key: T) -> None:
        await self.unsubscribe(socket, (key,))
    
    async def _send(self, sockets: Iterable[WebSocket], data: dict[str, Any]) -> None:
        dead_connections = []

        for socket in sockets:
            try:
                await socket.send_json(jsonable_encoder(data))
            except WebSocketDisconnect:
                dead_connections.append(socket)
            except Exception:
                logger.exception('Error sending data over web socket')
                dead_connections.append(socket)

        if dead_connections:
            await self.disconnect(dead_connections)

    async def send_to_subscribers(self, key: T, data: dict[str, Any]) -> None:
        logger.debug('Sending %s to %s', data, key)

        async with self.lock:
            sockets = list(self.sockets_by_subscription.get(key, ()))

        if sockets:
            await self._send(sockets, data)

    async def broadcast(self, data: dict[str, Any]) -> None:
        logger.debug('Broadcasting %s', data)

        async with self.lock:
            sockets = list(self.subscriptions_by_socket)
        
        if sockets:
            await self._send(sockets, data)
        