#!/usr/bin/env bash

# Stop only hadoop java processes (do NOT match folder names)
pkill -9 -f "org.apache.hadoop" || true
rm -f /tmp/hadoop-*.pid

JAVA_PATH="/usr/lib/jvm/java-8-openjdk-amd64"
HADOOP_DIR="$HOME/hadoop"
HIVE_DIR="$HOME/hive"

# Ensure hadoop-env.sh has JAVA_HOME
sed -i '/export JAVA_HOME=/d' "$HADOOP_DIR/etc/hadoop/hadoop-env.sh"
echo "export JAVA_HOME=$JAVA_PATH" >> "$HADOOP_DIR/etc/hadoop/hadoop-env.sh"

# Prepend exports to top of ~/.bashrc so non-interactive SSH gets them
sed -i '/# ---- BDA project ----/,$d' ~/.bashrc
sed -i '/export HADOOP_HOME=/d' ~/.bashrc
sed -i '/export JAVA_HOME=/d' ~/.bashrc
sed -i '/export HIVE_HOME=/d' ~/.bashrc
sed -i '/export PDSH_RCMD_TYPE=/d' ~/.bashrc

cat > ~/.bashrc.new <<EOF
export JAVA_HOME=$JAVA_PATH
export HADOOP_HOME=$HADOOP_DIR
export HIVE_HOME=$HIVE_DIR
export PDSH_RCMD_TYPE=ssh
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$HIVE_HOME/bin

EOF
cat ~/.bashrc >> ~/.bashrc.new
mv ~/.bashrc.new ~/.bashrc

# Ensure SSH service is running
sudo service ssh start || true

# Format HDFS freshly
"$HADOOP_DIR/bin/hdfs" namenode -format -force

# Start HDFS and YARN
"$HADOOP_DIR/sbin/start-dfs.sh"
"$HADOOP_DIR/sbin/start-yarn.sh"

# Leave safe mode
"$HADOOP_DIR/bin/hdfs" dfsadmin -safemode leave || true

# Hive directories
"$HADOOP_DIR/bin/hdfs" dfs -mkdir -p /tmp /user/hive/warehouse
"$HADOOP_DIR/bin/hdfs" dfs -chmod g+w /tmp /user/hive/warehouse

# Ensure Derby schema initialized
if [ ! -d "$HIVE_DIR/metastore_db" ]; then
    cd "$HIVE_DIR" && "$HIVE_DIR/bin/schematool" -dbType derby -initSchema || true
fi

# Ensure Mongo is running
sudo service mongod start || sudo systemctl start mongod || true

echo "=== ALL SERVICES CONFIGURED AND STARTED ==="
jps
