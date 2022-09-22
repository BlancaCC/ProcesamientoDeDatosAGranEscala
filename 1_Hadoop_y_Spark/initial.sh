echo "Task 1: Updating system"
yum -y update && yum clean all
echo "Task 2: Installing packages"
yum -y install wget
yum -y install which
yum -y install rsync
yum -y install java-1.7.0-openjdk
echo "Task 3: Hadoop installation (may take a while)"
wget https://archive.apache.org/dist/hadoop/common/hadoop-2.8.1/hadoop-2.8.1.tar.gz
tar xvzf hadoop-2.8.1.tar.gz
mv hadoop-2.8.1 opt/
cd opt/ && ln -s hadoop-2.8.1 hadoop
export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk
echo 'export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk' >> ./hadoop/etc/hadoop/hadoop-env.sh
./hadoop/bin/hadoop 

#cat opt/hadoop/etc/hadoop/hadoop-env.sh