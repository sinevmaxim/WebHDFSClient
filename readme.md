# Web HDFS Client

## Setup

Download and install [Python](https://www.python.org/downloads/).

Download and install [Ubuntu](https://ubuntu.com/download/desktop)

Download and install [Hadoop](https://www.tutorialspoint.com/hadoop/hadoop_enviornment_setup.htm)

Run this followed commands:

```bash
git clone https://github.com/sinevmaxim/WebHDFSClient.git

$HADOOP_HOME/opt/hadoop/sbin/start-dfs.sh

$HADOOP_HOME/opt/hadoop/sbin/start-yarn.sh

nano conf.json
# INSERT YOUR HADOOP HDFS IP ADDRES, PORT AND USER

sudo nano /private/etc/hosts
# ADD LOCAL HADOOP HOST ADDRESS AND HOST NAME

python3 web_hdfs_client.py
```
