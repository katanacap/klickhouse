# Justfile

# По умолчанию запускаем полную проверку (check)
default: check

# Полная проверка: fmt (проверка), clippy, тесты
check:
    @echo "==> Checking format..."
    cargo fmt --all -- --check

    @echo "==> Checking clippy..."
    cargo clippy --all-targets --all-features -- -D warnings

    @echo "==> Running tests with nextest..."
    cargo nextest run --workspace --all-features

    @echo "All checks passed!"

# Форматирование кода
fmt:
    @echo "==> Formatting code..."
    cargo fmt --all

# Запуск Clippy
clippy:
    @echo "==> Clippy linting..."
    cargo clippy --all-targets --all-features -- -D warnings

# Запуск тестов (nextest)
test:
    @echo "==> Running tests (debug)..."
    cargo nextest run --workspace --all-features

# Тесты в release-сборке
test-release:
    @echo "==> Running tests (release)..."
    cargo nextest run --workspace --all-features --release

# Генерация (и открытие) документации
doc:
    @echo "==> Building docs..."
    cargo doc --no-deps --all-features --open

# Проверка устаревших зависимостей
outdated:
    @echo "==> Checking outdated dependencies..."
    cargo outdated

# Обновление зависимостей
update:
    @echo "==> Updating dependencies..."
    cargo update