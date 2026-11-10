# Installation Guide

This guide installs everything from scratch on a Windows laptop using **WSL2 (Ubuntu 22.04)**. It is exactly the setup this project was built and tested on.

## Requirements

| Item | Version used | Notes |
|---|---|---|
| Windows | 10 / 11 with WSL2 | 8 GB RAM is enough; ~5 GB free disk |
| Ubuntu (WSL) | 22.04.5 LTS (jammy) | A separate distro just for this project |
| Java | OpenJDK 1.8 (8) | Hive 3.1.3 needs Java 8 |
| Hadoop | 3.3.6 | HDFS + YARN + MapReduce, pseudo-distributed (single node) |
| Hive | 3.1.3 | Embedded Derby metastore |
| MongoDB | 8.0 (+ mongosh) | Live data source and result store |
| Python | 3.10 (Ubuntu default) | `pip3 install -r requirements.txt` |

---

## 1. Create a separate Ubuntu for the project (PowerShell)
```powershell
wsl --install -d Ubuntu-22.04        # restart Windows if asked, then create a username/password
wsl --set-default Ubuntu-22.04
```
Everything below runs **inside Ubuntu** (prompt `user@host:~$`). To remove everything later: `wsl --unregister Ubuntu-22.04` (deletes the whole distro).

> If you also have Docker Desktop, make sure you are not in its `docker-desktop` shell (prompt `-sh` / `/mnt/host/...`) — type `exit` and open Ubuntu-22.04.

## 2. Java 8 and basic tools
```bash
cd ~
sudo apt update
sudo apt install -y openjdk-8-jdk python3 python3-pip python3-venv ssh pdsh wget unzip curl gnupg
java -version        # openjdk version "1.8.0_..."
```

## 3. Passwordless SSH (Hadoop's start scripts need it)
```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
sudo service ssh start
ssh localhost exit   # type "yes" once; must NOT ask for a password
```

## 4. Download Hadoop and Hive
```bash
cd ~
wget -c https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz && mv hadoop-3.3.6 hadoop
wget -c https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
tar -xzf apache-hive-3.1.3-bin.tar.gz && mv apache-hive-3.1.3-bin hive
rm -f *.tar.gz *.tgz
```

## 5. Environment variables
```bash
cat >> ~/.bashrc <<'EOF'

# ---- BDA project ----
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export PDSH_RCMD_TYPE=ssh
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HIVE_HOME=$HOME/hive
export PATH=$PATH:$HIVE_HOME/bin
EOF
source ~/.bashrc
hadoop version       # Hadoop 3.3.6
```

## 6. Configure Hadoop (pseudo-distributed)
```bash
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
mkdir -p ~/hadoop_data

cat > $HADOOP_HOME/etc/hadoop/core-site.xml <<EOF
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>$HOME/hadoop_data</value></property>
</configuration>
EOF

cat > $HADOOP_HOME/etc/hadoop/hdfs-site.xml <<EOF
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
</configuration>
EOF

cat > $HADOOP_HOME/etc/hadoop/mapred-site.xml <<EOF
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
  <property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
  <property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
</configuration>
EOF

cat > $HADOOP_HOME/etc/hadoop/yarn-site.xml <<EOF
<configuration>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
</configuration>
EOF
```
| Setting | Why |
|---|---|
| `fs.defaultFS` | HDFS address used by every tool (`hdfs://localhost:9000`) |
| `hadoop.tmp.dir` | Keeps HDFS data in the home folder so it survives restarts |
| `dfs.replication = 1` | Only one DataNode on a single machine |
| `*.env HADOOP_MAPRED_HOME` | Prevents the common "cannot find MRAppMaster" error |
| `mapreduce_shuffle` | Enables the shuffle service for MapReduce on YARN |

Format HDFS (**first time only — never again, it erases HDFS**) and start Hadoop:
```bash
hdfs namenode -format
start-dfs.sh && start-yarn.sh
jps     # NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager
```
Web UIs: HDFS http://localhost:9870 · YARN http://localhost:8088

## 7. Configure Hive 3.1.3
```bash
# Hive 3.1.3 ships Guava 19, which crashes with Hadoop 3.3.6 - replace it with Hadoop's Guava 27
rm $HIVE_HOME/lib/guava-19.0.jar
cp $HADOOP_HOME/share/hadoop/common/lib/guava-27.0-jre.jar $HIVE_HOME/lib/

cat > $HIVE_HOME/conf/hive-site.xml <<EOF
<configuration>
  <property><name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=$HOME/hive/metastore_db;create=true</value></property>
  <property><name>hive.metastore.warehouse.dir</name><value>/user/hive/warehouse</value></property>
  <property><name>hive.metastore.schema.verification</name><value>false</value></property>
  <property><name>hive.cli.print.header</name><value>true</value></property>
</configuration>
EOF

hdfs dfs -mkdir -p /tmp /user/hive/warehouse
hdfs dfs -chmod g+w /tmp /user/hive/warehouse
cd ~ && schematool -dbType derby -initSchema      # ends with "schemaTool completed"
hive -e "show databases;"                         # prints: default
```
The metastore uses embedded **Derby** with an absolute path, so `hive` works from any folder (only one Hive session at a time).

## 8. Install MongoDB 8.0
```bash
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/8.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
sudo apt update && sudo apt install -y mongodb-org
sudo systemctl start mongod && sudo systemctl enable mongod
mongosh --eval "db.runCommand({ping:1})"      # { ok: 1 }
```

## 9. Python libraries
```bash
pip3 install -r requirements.txt
```
(If pip says "externally-managed-environment", add `--break-system-packages`.)

## 10. Get the project and the dataset
```bash
mkdir -p ~/bda && cd ~/bda
git clone https://github.com/Nithin0029/bda-live-movie-ratings-hadoop-hive.git p3_movie_live
cd p3_movie_live
wget https://files.grouplens.org/datasets/movielens/ml-latest-small.zip && unzip ml-latest-small.zip
mkdir -p results
chmod +x *.py run_pipeline.sh
```

## Every time you open Ubuntu
```bash
sudo service ssh start
start-dfs.sh && start-yarn.sh
jps
sudo systemctl start mongod     # usually already running
```
Now follow [RUNNING.md](RUNNING.md).

## Troubleshooting
| Problem | Fix |
|---|---|
| `jps` missing NameNode / DataNode | `stop-dfs.sh`, then `sudo service ssh start` and `start-dfs.sh` |
| `Connection refused localhost:9000` | HDFS not running → `start-dfs.sh` |
| `Name node is in safe mode` | Wait 30 s, or `hdfs dfsadmin -safemode leave` |
| Job stuck at "Running job" | YARN not running → `start-yarn.sh` |
| `Could not find or load main class ...MRAppMaster` | Check the three `*.env` properties in mapred-site.xml |
| `pymongo ... Connection refused :27017` | `sudo systemctl start mongod` |
| `cat: Unable to write to output stream` | Harmless (`head` closed the pipe) |
| Hive metastore / Derby lock error | Only one Hive session at a time - close other `hive` windows |
| `NoSuchMethodError ... Preconditions.checkArgument` | Guava clash - redo the Guava step in section 7 |
