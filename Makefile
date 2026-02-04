compose-up:
	docker-compose up --build -d

compose-down:
	docker-compose down

logs:
	docker-compose logs -f

test:
	pytest -v
