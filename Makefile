.PHONY: up down build logs ps clean test-seed

# Start all platform services in the background
up:
	@echo "🚀 Spinning up IICPC Distributed Benchmarking Platform..."
	docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --build
	@echo "✅ Services started. Open http://localhost:3000 to view dashboard."

# Stop and clean all platform services (including volumes)
down:
	@echo "🛑 Stopping and destroying all containers/networks/volumes..."
	docker compose -f docker-compose.yml -f docker-compose.local.yml down -v
	@echo "✅ Cleanup complete."

# Rebuild all container images from scratch
build:
	@echo "🛠️ Rebuilding all Docker images..."
	docker compose -f docker-compose.yml -f docker-compose.local.yml build --no-cache
	@echo "✅ Build complete."

# Follow system logs in real-time
logs:
	docker compose -f docker-compose.yml -f docker-compose.local.yml logs -f

# List all running service containers
ps:
	docker compose -f docker-compose.yml -f docker-compose.local.yml ps

# Stop containers, remove temp builds, clean state
clean: down
	@echo "🧹 Cleaning up temporary build directories..."
	rm -rf core-orchestrator/temp_builds/*
	@echo "✅ Workspace cleaned."

# Seed the Postgres DB manually if needed
seed:
	@echo "🌱 Seeding PostgreSQL databases..."
	docker exec -i postgres psql -U postgres -d benchmarking < telemetry-config/init.sql
	@echo "✅ DB Seeded successfully."
