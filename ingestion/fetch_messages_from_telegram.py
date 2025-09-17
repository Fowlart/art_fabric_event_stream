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

if __name__ == '__main__':

    dotenv.load_dotenv()

    telegrammer = Telegramer()

    dialog_of_interest = os.getenv("SPECIFIC_DIALOG")

    telegrammer.print_dialogs()

    messages: list[dict[str,Any]] = telegrammer.get_messages_from_dialog(dialog_of_interest,500)

    dir_for_storing = f"{os.getenv('MESSAGE_FOLDER')}\\{datetime.date.today()}"

    if not (os.path.isdir(dir_for_storing)):
        os.mkdir(dir_for_storing)

    for m in messages:
        file_name = ((str(m.get('dialog') + '_'+ m.get('crawling_date'))
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
