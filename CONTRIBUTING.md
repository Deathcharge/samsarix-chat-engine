# Contributing to Helix Chat Engine

We welcome contributions! Here's how to get started.

## Getting Started

1. Fork the repository
2. Clone your fork
3. Create a branch: `git checkout -b feature/your-feature`
4. Make changes and commit: `git commit -am 'Add feature'`
5. Push to branch: `git push origin feature/your-feature`
6. Submit a pull request

## Development Setup

```bash
git clone https://github.com/YOUR_USERNAME/helix-chat-engine.git
cd helix-chat-engine
pip install -e ".[dev]"
pip install -r requirements-test.txt
```

## Running Tests

```bash
pytest tests/ -v
pytest tests/ --cov
pytest tests/ -m websocket  # Run specific marker
```

## Coding Standards

- Follow PEP 8
- Use type hints
- Write comprehensive docstrings
- Minimum 80% test coverage
- Keep lines under 100 characters

## Pull Request Process

1. Ensure all tests pass
2. Add tests for new functionality
3. Update documentation
4. Provide clear description
5. Wait for review

## Code of Conduct

Please follow our [Code of Conduct](CODE_OF_CONDUCT.md).
