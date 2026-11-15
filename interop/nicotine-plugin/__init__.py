"""Lets a test drive a headless Nicotine+ through private messages.

A message that starts with "!" is a command about its sender:

    !browse                  browse the sender's shares
    !info                    ask for the sender's user info
    !search TERM             search the sender's shares
    !get VIRTUAL_PATH|SIZE   download a file from the sender

What comes back is logged on lines that contain INTEROP, which run.sh reads.
"""

from pynicotine.events import events
from pynicotine.pluginsystem import BasePlugin


class Plugin(BasePlugin):

    def loaded_notification(self):
        events.connect("shared-file-list-response", self._browsed)
        events.connect("user-info-response", self._info)
        events.connect("file-search-response", self._found)
        self.log("INTEROP plugin loaded")

    def incoming_private_chat_notification(self, user, line):
        if not line.startswith("!"):
            return
        command, _, argument = line[1:].partition(" ")
        self.log("INTEROP command from %s: %s %s", (user, command, argument))
        if command == "browse":
            self.core.userbrowse.browse_user(user, new_request=True, switch_page=False)
        elif command == "info":
            self.core.userinfo.show_user(user, refresh=True, switch_page=False)
        elif command == "search":
            self.core.search.do_search(argument, "user", users=[user], switch_page=False)
        elif command == "get":
            virtual_path, _, size = argument.rpartition("|")
            self.core.downloads.enqueue_download(user, virtual_path, size=int(size))

    def _browsed(self, msg):
        folders = msg.list or []
        files = sum(len(folder_files) for _folder, folder_files in folders)
        self.log("INTEROP browsed %s: %s folders, %s files", (msg.username, len(folders), files))

    def _info(self, msg):
        self.log("INTEROP info %s: %r, %s slots, queue of %s",
                 (msg.username, msg.descr, msg.totalupl, msg.queuesize))

    def _found(self, msg):
        names = [entry[1] for entry in (msg.list or [])]
        self.log("INTEROP found %s: %s", (msg.username, names))

    def download_finished_notification(self, user, virtual_path, real_path):
        self.log("INTEROP downloaded from %s: %s", (user, virtual_path))
