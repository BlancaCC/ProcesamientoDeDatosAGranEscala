all: build

build: 
	sudo docker build -t hadoop .
run: 
	docker exec -P  -it hadoop  sh
detachedRun:
	docker run -d -p 50070:50070 hadoop
compile:
	cd entrega && make compile &&  cd ..


