import os
from typing import Any

from telethon import TelegramClient
import dotenv as d
import datetime as dt

class Telegramer():

    def _get_tg_client(self) -> TelegramClient:
        d.load_dotenv()
        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        return TelegramClient("init_session", int(api_id), api_hash)

    def __init__(self):
        self.tg_client = self._get_tg_client()

    # print dialogs
    async def _print_dialogs(self):
        async for dialog in self.tg_client.iter_dialogs():
            print(dialog.name, "has id", dialog.id)

    def print_dialogs(self):
        with self.tg_client:
            self.tg_client.loop.run_until_complete(self._print_dialogs())

    # print messages from dialog
    async def _print_messages_from_dialog(self, dialog_id: str):
        async for m in self.tg_client.iter_messages(entity=dialog_id,limit=100):
            print(m)

    def print_messages_from_dialog(self, dialog_id: str):
        with self.tg_client:
            self.tg_client.loop.run_until_complete(self._print_messages_from_dialog(dialog_id))


    # read and save messages from dialog
    # todo: make it to return the DTOs
    async def _get_messages_from_dialog(self, dialog_id: str, message_limit: int = 100) -> list[dict[str,Any]]:
        message_list: list[dict[str,Any]] = []
        async for m in self.tg_client.iter_messages(entity=dialog_id,limit=message_limit):

            try:
                sender_dict: dict[Any, Any] = m.sender.to_dict()
                user = {str(k): str(v) for k, v in sender_dict.items()}
            except Exception as e:
                print(f"Error processing sender: {e}")
                user = "no info"

            # Data to be written
            json_record = {
                    "crawling_date": str(dt.datetime.now()),
                    "message_date": str(m.date.date()),
                    "message_text": m.message,
                    "dialog": m.sender.title,
                    "post_author": m.post_author,
                    "is_channel": m.is_channel,
                    "is_group": m.is_group,
                    "user": user
            }
            message_list.append(json_record)
        return message_list


    def get_messages_from_dialog(self,
                                 dialog_id: str,
                                 message_limit: int = 100) -> list[dict[str,Any]]:
        with self.tg_client:
            message_list: list[dict[str,Any]] = self.tg_client.loop.run_until_complete(self._get_messages_from_dialog(dialog_id, message_limit))
        return message_list

