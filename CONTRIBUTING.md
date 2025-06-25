# Contributing to DynamoDB PartiQL JDBC Driver

Thank you for your interest in contributing to this project! We welcome contributions from the community.

## 🤝 How to Contribute

### Reporting Issues

1. **Check existing issues** first to avoid duplicates
2. **Use the issue template** to provide all necessary information
3. **Include reproduction steps** for bugs
4. **Provide clear descriptions** for feature requests

### Pull Requests

1. **Fork the repository** and create a feature branch
2. **Follow the coding standards** (Google Java Format)
3. **Write tests** for your changes
4. **Ensure all tests pass** before submitting
5. **Update documentation** if needed

## 🔧 Development Setup

### Prerequisites

- Java 21 or higher
- Maven 3.8+
- Docker (for integration tests)
- Git

### Getting Started

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/dynamodb-partiql-jdbc.git
cd dynamodb-partiql-jdbc

# Build the project
./mvnw clean compile

# Run unit tests
./mvnw test

# Run all tests including integration tests
./mvnw verify

# Format code (required before commits)
./mvnw spotless:apply

# Check code formatting
./mvnw spotless:check
```

## 📝 Coding Standards

- **Follow Google Java Format** - Run `./mvnw spotless:apply`
- **Write comprehensive tests** - Aim for 80%+ code coverage
- **Use descriptive variable names** - Avoid abbreviations
- **Add Javadoc** for public APIs
- **Follow existing patterns** in the codebase

## 🧪 Testing

- **Unit tests** for business logic
- **Integration tests** using Testcontainers with DynamoDB Local
- **All tests must pass** before submission
- **Add tests for new features** and bug fixes

## 📋 Pull Request Process

1. **Create a feature branch** from `main`
2. **Make your changes** following coding standards
3. **Add or update tests** as needed
4. **Run the full test suite** to ensure nothing breaks
5. **Format your code** with `./mvnw spotless:apply`
6. **Commit with clear messages** describing the changes
7. **Push to your fork** and create a pull request
8. **Address any feedback** from code review

## 🔍 Code Review

- All contributions require code review
- Maintainers will review PRs and provide feedback
- Address feedback promptly
- Be respectful and constructive in discussions

## 📜 License

By contributing to this project, you agree that your contributions will be licensed under the MIT License.

## ❓ Questions?

If you have questions about contributing, feel free to:
- Open an issue for discussion
- Ask questions in pull request comments
- Reach out to the maintainers

Thank you for contributing! 🎉