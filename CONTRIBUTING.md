# Contributing to AOS-Farm

Thank you for your interest in contributing to AOS-Farm! This document provides guidelines and best practices for contributing.

## Getting Started

### Prerequisites

- Git
- Node.js 18+ or Python 3.10+ (depending on implementation choice)
- Supabase account
- Access to autonomOS services (AOD, AAM, DCL, Agent Orchestrator)

### Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd AOS-Farm
   ```

2. **Set up the database**:
   ```bash
   cd database
   supabase link --project-ref your-project-ref
   supabase db push
   ```

3. **Configure environment**:
   ```bash
   cp config/.env.example .env
   # Edit .env with your credentials
   ```

4. **Install dependencies**:

   Backend:
   ```bash
   cd backend
   npm install  # or pip install -r requirements.txt
   ```

   Frontend:
   ```bash
   cd frontend
   npm install
   ```

5. **Run tests**:
   ```bash
   cd backend
   npm test
   ```

---

## Development Workflow

### Branching Strategy

- `main` - Production-ready code
- `develop` - Integration branch for features
- `feature/<name>` - Feature branches
- `fix/<name>` - Bug fix branches

### Making Changes

1. **Create a branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**

3. **Test your changes**:
   ```bash
   npm test
   npm run test:integration
   ```

4. **Commit with clear messages**:
   ```bash
   git commit -m "Add: new scenario for extreme data quality"
   ```

5. **Push and create a Pull Request**:
   ```bash
   git push origin feature/your-feature-name
   ```

---

## Code Standards

### TypeScript/JavaScript

- Use TypeScript for type safety
- Follow Airbnb style guide
- Use ESLint and Prettier
- Write JSDoc comments for public APIs

```typescript
/**
 * Generate synthetic customer data for a lab tenant.
 *
 * @param tenantId - The lab tenant ID
 * @param count - Number of customers to generate
 * @param sources - Source systems to generate for
 * @returns Promise that resolves when generation is complete
 */
async function generateCustomers(
  tenantId: string,
  count: number,
  sources: string[]
): Promise<void> {
  // Implementation
}
```

### Python

- Follow PEP 8 style guide
- Use type hints
- Use Black for formatting
- Write docstrings for all functions

```python
async def generate_customers(
    tenant_id: str,
    count: int,
    sources: list[str]
) -> None:
    """
    Generate synthetic customer data for a lab tenant.

    Args:
        tenant_id: The lab tenant ID
        count: Number of customers to generate
        sources: Source systems to generate for
    """
    # Implementation
```

### SQL

- Use lowercase for keywords
- Indent for readability
- Always include comments
- Include migration version and description

---

## Testing

### Unit Tests

Test individual functions and classes:

```typescript
describe('CustomerGenerator', () => {
  test('generates correct number of customers', async () => {
    const generator = new CustomerGenerator(mockDb);
    await generator.generate('test-tenant', 100, ['crm']);

    expect(mockDb.insert).toHaveBeenCalledWith(
      'synthetic_customers',
      expect.arrayContaining([
        expect.objectContaining({ lab_tenant_id: 'test-tenant' })
      ])
    );
  });
});
```

### Integration Tests

Test component interactions:

```typescript
describe('E2E Run', () => {
  test('completes successfully with clean scenario', async () => {
    const orchestrator = new RunOrchestrator();
    const run = await orchestrator.startRun('e2e-small-clean');

    await waitForCompletion(run.id);

    const status = await orchestrator.getRunStatus(run.id);
    expect(status.status).toBe('success');
  });
});
```

### E2E Tests

Test full user flows through the UI.

---

## Adding New Features

### 1. Adding a New Scenario

See [scenarios/README.md](scenarios/README.md)

### 2. Adding a New Synthetic Data Generator

1. Create generator class in `backend/src/synthetic-data/generators/<type>/`
2. Implement generator interface
3. Register in `SyntheticDataEngine`
4. Add to scenario config schema
5. Write tests
6. Update documentation

### 3. Adding a New Chaos Pattern

1. Create chaos module in `backend/src/chaos/`
2. Implement chaos interface
3. Register in `ChaosEngine`
4. Add to scenario chaos config
5. Write tests
6. Update documentation

### 4. Adding a New API Endpoint

1. Create route in `backend/src/api/routes/`
2. Create controller method
3. Add validation middleware
4. Write tests
5. Update `docs/api-spec.md`

---

## Database Changes

### Creating a Migration

1. Create new migration file:
   ```bash
   touch database/migrations/00X_description.sql
   ```

2. Write SQL:
   ```sql
   -- Migration description
   -- Version: 00X

   CREATE TABLE new_table (...);

   INSERT INTO schema_version (version, description)
   VALUES (X, 'Description');
   ```

3. Test locally:
   ```bash
   supabase db reset
   ```

4. Commit migration file

---

## Documentation

### When to Update Documentation

- Adding new features
- Changing API contracts
- Updating configuration options
- Fixing bugs that affect usage

### Documentation Files

- `README.md` - Project overview
- `docs/architecture.md` - System design
- `docs/api-spec.md` - API documentation
- `docs/scenarios.md` - Scenario authoring guide
- `docs/integration.md` - Integration guide
- Module READMEs in each directory

---

## Pull Request Process

1. **Ensure tests pass**:
   ```bash
   npm test
   npm run lint
   ```

2. **Update documentation** if needed

3. **Write a clear PR description**:
   - What does this PR do?
   - Why is it needed?
   - How was it tested?
   - Any breaking changes?

4. **Request review** from at least one team member

5. **Address feedback** and make necessary changes

6. **Squash commits** if requested

7. **Merge** when approved

---

## Commit Message Guidelines

Use conventional commit format:

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types**:
- `feat` - New feature
- `fix` - Bug fix
- `docs` - Documentation changes
- `style` - Code style changes (formatting)
- `refactor` - Code refactoring
- `test` - Adding tests
- `chore` - Maintenance tasks

**Examples**:
```
feat(synthetic-data): add invoice generator

Implements generator for synthetic invoice records with
configurable volume and chaos injection.

Closes #123
```

```
fix(orchestrator): handle timeout in AAM stage

Added proper timeout handling and error recovery when
AAM stage exceeds configured timeout.
```

---

## Code Review Guidelines

### As a Reviewer

- Be constructive and respectful
- Focus on code quality, not personal preferences
- Test the changes if possible
- Approve when ready, request changes if needed

### As an Author

- Be open to feedback
- Explain your design decisions
- Respond to all comments
- Make requested changes or explain why not

---

## Getting Help

- Check existing documentation
- Search existing issues
- Ask in team chat
- Create a new issue for bugs or feature requests

---

## License

By contributing to AOS-Farm, you agree that your contributions will be licensed under the same license as the project.
