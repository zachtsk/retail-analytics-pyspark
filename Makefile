rebuild:
	docker exec docker_jupyter bash -c "./scripts/rebuild_package.sh"

testing: rebuild
	docker exec docker_jupyter bash -c "pytest"

init:
	docker-compose -f docker-compose.yml up -d --build

up:
	docker-compose -f docker-compose.yml up -d

down:
	docker-compose -f docker-compose.yml down

build:
	docker-compose -f docker-compose.yml build