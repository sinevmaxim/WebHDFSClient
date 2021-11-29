"""
    WebHDFS Client for Stand-Alone Hadoop System (Runned on the same machine)

    Property dfs.webhdfs.enabled in hdfs-site.xml must be true
    conf.json incapsulates all cofiguration data such as ip, port, user

    Author : Sinev Max
"""

import requests
import json
import sys
import os


class WebHDFSClient:
    """
        Reading data from config file and creating some constant variables
    """

    def __init__(self):
        with open('conf.json') as json_file:
            self.CONFIG = json.load(json_file)

        self.HOST = self.CONFIG["HOST"]
        self.PORT = self.CONFIG["PORT"]
        self.USER = self.CONFIG["USER"]
        self.DIRECTORY_ROOT = "webhdfs/v1/"
        self.current_directory = self.DIRECTORY_ROOT

    """
        Creating infinite loop where user commands are readed
    """

    def run(self):
        _command = ''
        while _command != "exit":
            _command = input(f"{self.USER}:~/{self.current_directory}$ ")
            splitted_command = _command.split(" ")

            command = splitted_command[0]
            args = {}

            if len(splitted_command) > 1:
                args["file"] = splitted_command[1]
                args["directory"] = splitted_command[1]

            if len(splitted_command) > 2:
                args["options"] = splitted_command[2:]

            if command == "ls":
                self.ls(args)

            if command == "rm":
                self.rm(args)

            if command == "cd":
                self.cd(args)

            if command == "mkdir":
                self.mkdir(args)

            if command == "put":
                self.put(args)

            if command == "get":
                self.get(args)

            if command == "lls":
                self.lls(args)

            if command == "lcd":
                self.lcd(args)

    """
        Get all files/directories in current directory in HDFS
    """

    def ls(self, args):
        params = {
            'user.name': self.USER,
            'op': "LISTSTATUS"
        }

        url = f"http://{self.HOST}:{self.PORT}/{self.current_directory}"

        response = requests.get(url, params=params)

        _data = json.loads(response.text)["FileStatuses"]["FileStatus"]

        if len(_data) > 0:
            for file in _data:
                print(file["pathSuffix"])

    """
        Change directory in HDFS
    """

    def cd(self, args):
        params = {
            'user.name': self.USER,
            'op': "LISTSTATUS"
        }

        _directories = args["directory"].strip("/")
        directories = _directories.split("/")

        for directory in directories:

            if directory == "..":
                directories_list = self.current_directory.split('/')

                if directories_list[-2] == "v1":
                    break

                self.current_directory = "/".join(
                    directories_list[0:-2]) + "/"

                flag = True

            url = f"http://{self.HOST}:{self.PORT}/{self.current_directory}"
            response = requests.get(url, params=params)

            _data = json.loads(response.text)[
                "FileStatuses"]["FileStatus"] if response.status_code != 404 else []

            flag = True
            if len(_data) > 0:
                for data in _data:
                    if data["pathSuffix"] == directory and data["type"] == "DIRECTORY":
                        self.current_directory = self.current_directory + directory + "/"
                        flag = False

            if flag:
                print("No such directory")

    """
        Create directory in current directory in HDFS
    """

    def mkdir(self, args):
        params = {
            'user.name': self.USER,
            'op': "MKDIRS"
        }

        url = f"http://{self.HOST}:{self.PORT}/{self.current_directory}{args['directory'].strip('/')}"

        requests.put(url, params=params)

    """
        Create file in current directory in HDFS
    """

    def put(self, args):
        params = {
            'user.name': self.USER,
            'op': "CREATE"
        }

        url = f"http://{self.HOST}:{self.PORT}/{self.current_directory}{args['file']}"
        print(url)

        response = requests.put(url, params=params)

        if response.status_code == 201:
            self.append(args=args)

    """
        Download file from current directory in HDFS
    """

    def get(self, args):
        params = {
            'user.name': self.USER,
            'op': "OPEN"
        }

        # self.current_directory = self.DIRECTORY_ROOT + self.current_directory
        url = f"http://{self.HOST}:{self.PORT}/{self.current_directory}{args['file']}"
        print(url)

        response = requests.get(url, params=params)

        if response.status_code == 200:
            file = open(args['file'], "w")
            file.write(response.text)
            file.close()

    """
        Appending data to created file in HDFS
    """

    def append(self, args):
        params = {
            'user.name': self.USER,
            'op': "APPEND"
        }

        url = f"http://{self.HOST}:{self.PORT}/{self.current_directory}{args['file']}"
        files = {'upload_file': open(args['file'], 'rb')}

        request = requests.post(url, files=files, params=params, timeout=5)

    """
        Remove file/directory from current directory in HDFS
    """

    def rm(self, args):
        params = {
            'user.name': self.USER,
            'op': "DELETE"
        }

        # self.current_directory = self.DIRECTORY_ROOT + self.current_directory
        url = f"http://{self.HOST}:{self.PORT}/{self.current_directory}{args['file']}"

        response = requests.delete(url, params=params)

    """
        List of files/directories in current local directory
    """

    def lls(self, args):
        local_directories = os.listdir(os.getcwd())

        for file in local_directories:
            print(file)

    """
        Change local directory
    """

    def lcd(self, args):
        try:
            os.chdir(args["directory"])
        except FileNotFoundError:
            print(f"> Error: {sys.exc_info()}")


if __name__ == "__main__":

    """ 
        Initalizing WebHDFS Client
    """

    web_hdfs_client = WebHDFSClient()
    web_hdfs_client.run()
