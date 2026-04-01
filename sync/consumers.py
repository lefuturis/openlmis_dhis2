"""
WebSocket Consumer for real-time sync progress updates.
"""
import json
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.db import database_sync_to_async
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from loguru import logger


SYNC_PROGRESS_GROUP = 'sync_progress'


class SyncProgressConsumer(AsyncWebsocketConsumer):
    """
    WebSocket consumer that sends real-time sync progress to the admin UI.
    
    Clients join the 'sync_progress' group on connect and receive updates
    as Celery tasks progress through facilities.
    """

    async def connect(self):
        """Accept WebSocket connection and join sync_progress group."""
        # Only allow authenticated staff users
        user = self.scope.get('user')
        if user and user.is_authenticated and user.is_staff:
            await self.channel_layer.group_add(
                SYNC_PROGRESS_GROUP,
                self.channel_name
            )
            await self.accept()
            logger.info(f"WebSocket connected: {self.channel_name}")
        else:
            await self.close()
            logger.warning("WebSocket connection rejected: not authenticated staff")

    async def disconnect(self, close_code):
        """Leave sync_progress group on disconnect."""
        await self.channel_layer.group_discard(
            SYNC_PROGRESS_GROUP,
            self.channel_name
        )
        logger.info(f"WebSocket disconnected: {self.channel_name}")

    async def receive(self, text_data):
        """
        Handle incoming messages from the client.
        Supports starting a manual sync.
        """
        try:
            data = json.loads(text_data)
            action = data.get('action')

            if action == 'start_sync':
                year = data.get('year')
                month = data.get('month')
                if year and month:
                    period = f"{year}{int(month):02d}"
                    task_id = await self.dispatch_sync(period)
                    await self.send(text_data=json.dumps({
                        'type': 'sync_dispatched',
                        'period': period,
                        'task_id': task_id,
                        'message': f'Synchronisation lancée pour {month}/{year}',
                    }))
                else:
                    await self.send(text_data=json.dumps({
                        'type': 'error',
                        'message': 'Année et mois requis',
                    }))
            elif action == 'ping':
                await self.send(text_data=json.dumps({'type': 'pong'}))
        except json.JSONDecodeError:
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': 'Format JSON invalide',
            }))

    @database_sync_to_async
    def dispatch_sync(self, period):
        """Dispatch the manual sync Celery task."""
        from .tasks import manual_sync_task
        result = manual_sync_task.delay(period=period)
        return result.id

    # =========================================================================
    # Channel layer message handlers
    # These methods are called when the channel layer sends messages to this
    # consumer via group_send(). The 'type' field in the message is converted
    # to a method name (dots/hyphens → underscores).
    # =========================================================================

    async def sync_update(self, event):
        """Forward sync progress update to WebSocket client."""
        await self.send(text_data=json.dumps(event['data']))

    async def sync_started(self, event):
        """Notify client that sync has started."""
        await self.send(text_data=json.dumps(event['data']))

    async def sync_facility_update(self, event):
        """Forward per-facility progress to client."""
        await self.send(text_data=json.dumps(event['data']))

    async def sync_finished(self, event):
        """Notify client that sync is complete."""
        await self.send(text_data=json.dumps(event['data']))


def send_sync_progress(data: dict, msg_type: str = 'sync_update'):
    """
    Helper function to send sync progress from Celery tasks.
    
    This is the bridge between synchronous Celery tasks and the async
    channel layer. Call this from any Celery task to push real-time
    updates to connected admin browsers.
    
    Args:
        data: Dict with progress information
        msg_type: Channel layer message type (maps to consumer method)
    """
    channel_layer = get_channel_layer()
    if channel_layer is None:
        logger.warning("Channel layer not available, skipping progress update")
        return
    
    try:
        async_to_sync(channel_layer.group_send)(
            SYNC_PROGRESS_GROUP,
            {
                'type': msg_type,
                'data': data,
            }
        )
    except Exception as e:
        logger.warning(f"Failed to send sync progress via channel layer: {e}")
