#!/usr/bin/env bash
set -e

echo "=========================================================="
echo " Starting Automated Big Data Environment Setup"
echo " (Hadoop 3.3.6, Hive 3.1.3, MongoDB 8.0, Java 8, Python)"
echo "=========================================================="

echo "=== 1. Updating packages and installing prerequisites ==="
sudo apt update
sudo apt install -y openjdk-8-jdk python3 python3-pip python3-venv ssh pdsh wget unzip curl gnupg

echo "=== 2. Setting up Passwordless SSH for Hadoop ==="
mkdir -p ~/.ssh
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys
fi
sudo service ssh start || true
ssh -o StrictHostKeyChecking=no localhost exit 2>/dev/null || true

echo "=== 3. Downloading Hadoop 3.3.6 & Hive 3.1.3 ==="
cd "$HOME"
if [ ! -d "$HOME/hadoop" ]; then
    echo "Downloading Hadoop 3.3.6 (approx ~700 MB)..."
    wget -c https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
    tar -xzf hadoop-3.3.6.tar.gz && mv hadoop-3.3.6 hadoop
    rm -f hadoop-3.3.6.tar.gz
fi

if [ ! -d "$HOME/hive" ]; then
    echo "Downloading Hive 3.1.3 (approx ~300 MB)..."
    wget -c https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
    tar -xzf apache-hive-3.1.3-bin.tar.gz && mv apache-hive-3.1.3-bin hive
    rm -f apache-hive-3.1.3-bin.tar.gz
fi

echo "=== 4. Setting Environment Variables in ~/.bashrc ==="
if ! grep -q "HADOOP_HOME" "$HOME/.bashrc"; then
cat >> "$HOME/.bashrc" <<'EOF'

# ---- BDA project ----
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export PDSH_RCMD_TYPE=ssh
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HIVE_HOME=$HOME/hive
export PATH=$PATH:$HIVE_HOME/bin
EOF
fi

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export PDSH_RCMD_TYPE=ssh
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HIVE_HOME=$HOME/hive
export PATH=$PATH:$HIVE_HOME/bin

echo "=== 5. Configuring Hadoop (pseudo-distributed) ==="
# Ensure JAVA_HOME is exported inside hadoop-env.sh for SSH daemons
if ! grep -q "^export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" "$HADOOP_HOME/etc/hadoop/hadoop-env.sh"; then
    echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> "$HADOOP_HOME/etc/hadoop/hadoop-env.sh"
fi
mkdir -p "$HOME/hadoop_data"

cat > "$HADOOP_HOME/etc/hadoop/core-site.xml" <<EOF
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>$HOME/hadoop_data</value></property>
</configuration>
EOF

cat > "$HADOOP_HOME/etc/hadoop/hdfs-site.xml" <<EOF
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
</configuration>
EOF

cat > "$HADOOP_HOME/etc/hadoop/mapred-site.xml" <<EOF
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
  <property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
  <property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
</configuration>
EOF

cat > "$HADOOP_HOME/etc/hadoop/yarn-site.xml" <<EOF
<configuration>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
</configuration>
EOF

echo "=== 6. Configuring Hive (Guava patch & Derby Metastore) ==="
if [ -f "$HIVE_HOME/lib/guava-19.0.jar" ]; then
    rm -f "$HIVE_HOME/lib/guava-19.0.jar"
    cp "$HADOOP_HOME/share/hadoop/common/lib/guava-27.0-jre.jar" "$HIVE_HOME/lib/"
fi

cat > "$HIVE_HOME/conf/hive-site.xml" <<EOF
<configuration>
  <property><name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=$HOME/hive/metastore_db;create=true</value></property>
  <property><name>hive.metastore.warehouse.dir</name><value>/user/hive/warehouse</value></property>
  <property><name>hive.metastore.schema.verification</name><value>false</value></property>
  <property><name>hive.cli.print.header</name><value>true</value></property>
</configuration>
EOF

echo "=== 7. Installing MongoDB 8.0 ==="
if ! command -v mongod &> /dev/null; then
    curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor --yes
    echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/8.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
    sudo apt update
    sudo apt install -y mongodb-org
fi

sudo systemctl start mongod 2>/dev/null || sudo service mongod start 2>/dev/null || sudo service mongodb start 2>/dev/null || true

echo "=== 8. Formatting HDFS and Initializing Hive Metastore ==="
if [ ! -d "$HOME/hadoop_data/dfs/name" ]; then
    hdfs namenode -format -force
fi

start-dfs.sh && start-yarn.sh

# Ensure safe mode is turned off
hdfs dfsadmin -safemode leave 2>/dev/null || true

hdfs dfs -mkdir -p /tmp /user/hive/warehouse
hdfs dfs -chmod g+w /tmp /user/hive/warehouse

if [ ! -d "$HOME/hive/metastore_db" ]; then
    cd "$HOME" && schematool -dbType derby -initSchema
fi

echo "=== 9. Installing Python Dependencies ==="
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pip3 install --break-system-packages -r "$SCRIPT_DIR/requirements.txt" 2>/dev/null || pip3 install -r "$SCRIPT_DIR/requirements.txt"

chmod +x "$SCRIPT_DIR"/*.py "$SCRIPT_DIR"/*.sh 2>/dev/null || true

echo ""
echo "=========================================================="
echo " SETUP COMPLETE!"
echo " Everything is installed and configured:"
echo " - Hadoop 3.3.6 (HDFS & YARN running)"
echo " - Hive 3.1.3 (Derby metastore initialized)"
echo " - MongoDB 8.0 (Service running)"
echo " - Python libraries (pymongo, streamlit, pandas)"
echo "=========================================================="
