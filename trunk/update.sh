rm /tmp/mysqld.trace
sudo ~/mysql-5.1.34/client/mysqladmin shutdown

cd /usr/local/mysql/var/
sudo rm data.vdb
cd /usr/local/mysql/lib/mysql/plugin/
sudo rm *vogal*
sudo cp ~/mysql-5.1.34/storage/vogal/.libs/* .

cd ~/mysql-5.1.34/
./sql/mysqld --gdb --debug   &

cd storage/vogal

