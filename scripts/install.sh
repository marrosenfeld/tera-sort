sudo apt-add-repository -y ppa:webupd8team/java
sudo apt-get -y update
echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections
sudo apt-get -y install oracle-java7-installer 
export JAVA_HOME=/usr/lib/jvm/java-7-oracle
sudo apt-get -y install ssh
sudo apt-get -y install rsync
wget http://download.nextag.com/apache/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz
tar -xvf hadoop-2.7.2.tar.gz
mv hadoop-2.7.2 hadoop
