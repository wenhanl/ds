sudo rm /var/www/html/files/*
sudo chmod 777 /var/www/html/files
go run /home/ubuntu/ds/back/node.go $1 &
