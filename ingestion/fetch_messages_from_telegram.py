import codecs
import json
from typing import Any
import dotenv
import os
import datetime

from utils.telegramer import Telegramer

def _save_to_local_fs(path: str, json_record: dict[str,str]):
    with codecs.open(path, "w", "utf-8", "replace") as file:
        json_object = json.dumps(json_record, indent=2, separators=(',', ':'), ensure_ascii=False)
        file.write(json_object)
        file.close()

def save_in_common_file(messages: list[dict[str,Any]]):
    common_message = {"messages":str(messages)}
    _save_to_local_fs(path=f"{os.getenv('MESSAGE_FOLDER')}\\common_hist.json", json_record=common_message)

def save_in_separate_files(messages: list[dict[str,Any]]):
    dir_for_storing = f"{os.getenv('MESSAGE_FOLDER')}\\{datetime.date.today()}"
    if not (os.path.isdir(dir_for_storing)):
        os.mkdir(dir_for_storing)
    for m in messages:
        file_name = ((str(m.get('message_date'))
                       .replace(" ", "-"))
                       .replace("/", "-")
                       .replace("|","-")
                       .replace('"',"-")
                       .replace(":","")
                       .replace("✙", "")
                       .replace(".", "-")
                       .replace("--", "-")
                       .replace("_", "-"))
        print(file_name)
        _save_to_local_fs(f"{dir_for_storing}\\{file_name}.json",m)

if __name__ == '__main__':

    dotenv.load_dotenv()

    telegrammer = Telegramer()

    dialog_of_interest = os.getenv("SPECIFIC_DIALOG")

    telegrammer.print_dialogs()

    messages: list[dict[str,Any]] = telegrammer.get_messages_from_dialog(dialog_of_interest,100)

    save_in_common_file(messages)

    save_in_separate_files(messages)