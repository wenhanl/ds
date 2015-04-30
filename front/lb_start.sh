sqlite3 -line /home/ubuntu/ds/front/db/development.sqlite3 'delete from nodes;'
sqlite3 -line /home/ubuntu/ds/front/db/development.sqlite3 'delete from files;'
rails server &
go run /home/ubuntu/ds/back/lb_replica.go &
