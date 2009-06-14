sudo ~/mysql-5.1.34/client/mysqladmin shutdown
rm /tmp/mysqld.trace

cd /usr/local/mysql/var/
sudo rm data.vdb
cd /usr/local/mysql/lib/mysql/plugin/
sudo rm *vogal*
sudo cp ~/mysql-5.1.34/storage/vogal/.libs/* .

cd /usr/local/mysql/var/test/
rm *

cd ~/mysql-5.1.34/
./sql/mysqld --gdb --debug   &

cd storage/vogal

