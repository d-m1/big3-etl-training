install:
	docker compose build --no-cache
	docker-compose up -d

stop:
	docker-compose down -v

reset:
	docker-compose down -v
	docker-compose up -d