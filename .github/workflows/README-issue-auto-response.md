# Issue Auto-Response Agent

## Overview

This GitHub Actions workflow automatically responds to newly opened issues with helpful, context-aware information based on the issue content and project documentation.

## Purpose

The auto-response agent:
- Provides immediate assistance to users opening issues
- Analyzes issue content to detect common topics and questions
- Generates relevant responses with links to documentation and resources
- Reduces response time and improves user experience
- Helps triage issues by providing initial guidance

## How It Works

### Trigger
The workflow runs automatically whenever a new issue is opened in the repository (`on: issues: types: [opened]`).

### Process

1. **Checkout Repository**: Fetches the repository content to access documentation files
2. **Read Documentation**: Extracts key information from:
   - README.md (installation, features, usage)
   - ARCHITECTURE_DECISIONS.md (technical decisions)
   - CONTRIBUTING.md (contribution guidelines)
3. **Analyze Issue**: Examines the issue title and body for keywords related to:
   - Installation and setup
   - Database support
   - Performance and parallel execution
   - Replication modes (complete, incremental, atomic)
   - Configuration options
   - Error troubleshooting
   - Docker usage
4. **Generate Response**: Creates a contextual response in Spanish (matching the project's primary language) that includes:
   - Relevant information based on detected topics
   - Code examples and command snippets
   - Links to detailed documentation
   - Troubleshooting guidance
   - Request for additional details if needed
5. **Post Comment**: Automatically posts the generated response as a comment on the issue
6. **Add Label**: Tags the issue with `auto-responded` label for tracking

## Response Topics

The agent can detect and respond to questions about:

### Installation (📦)
- Download and installation instructions
- Docker deployment
- Java requirements

### Database Support (🗄️)
- Supported databases (Oracle, PostgreSQL, MySQL, MongoDB, etc.)
- JDBC driver configuration
- Database-specific features

### Performance (⚡)
- Parallel execution with `--jobs` parameter
- Fetch size optimization
- Bandwidth throttling
- Performance targets by table size

### Replication Modes (🔄)
- Complete mode (full table reload)
- Incremental mode (delta synchronization)
- Complete-atomic mode (staging tables)

### Configuration (⚙️)
- CLI arguments
- Configuration files
- Environment variable substitution

### Troubleshooting (🔍)
- Connection issues
- Permission requirements
- JDBC drivers
- Error diagnosis

### Docker Usage (🐳)
- Container deployment
- Volume mounting for configuration
- Network connectivity

## Example Response

When a user opens an issue with keywords like "install" and "postgres", the agent will respond with:

```markdown
👋 ¡Hola @username! Gracias por abrir este issue.

🤖 **Respuesta automática inicial del agente**

### 📦 Instalación
ReplicaDB requiere Java 11 o superior. Puedes instalarlo de las siguientes formas:
[installation instructions...]

### 🗄️ Bases de Datos Soportadas
ReplicaDB soporta una amplia gama de bases de datos:
- **Relacionales:** Oracle, PostgreSQL, MySQL/MariaDB...
[database information...]

### 📚 Recursos Adicionales
- 📖 [Documentación completa](https://osalvador.github.io/ReplicaDB/docs/docs.html)
[additional links...]
```

## Permissions

The workflow requires:
- `issues: write` - To post comments on issues
- `contents: read` - To read documentation files

## Configuration

The workflow is configured in `.github/workflows/issue-auto-response.yml` and runs automatically. No additional configuration is needed.

## Customization

To modify the response logic:

1. Edit the `Analyze issue and generate response` step in the workflow file
2. Update keyword detection in the `containsAny` checks
3. Modify response templates for each topic
4. Add new topic detection and response sections as needed

## Labels

The workflow automatically adds the `auto-responded` label to issues. Ensure this label exists in the repository, or GitHub will create it automatically on first use.

## Benefits

- **Immediate Response**: Users get instant feedback when opening issues
- **Consistent Information**: Ensures all users receive accurate, up-to-date information
- **Reduced Maintainer Load**: Handles common questions automatically
- **Improved Triage**: Pre-categorizes issues based on content
- **Better User Experience**: Users feel acknowledged and receive guidance immediately

## Limitations

- The agent provides general guidance based on keyword matching
- Complex or unique issues will still require human maintainer review
- Responses are in Spanish by default (matching the issue template language)
- Cannot access external resources or perform database testing

## Future Enhancements

Potential improvements:
- Integration with AI services (OpenAI, Anthropic) for more sophisticated responses
- Multi-language support based on issue language detection
- Code analysis for issues mentioning specific files or functions
- Integration with GitHub Discussions for FAQ responses
- Analytics on common issue topics to improve documentation

## Monitoring

Track the effectiveness of auto-responses by:
- Filtering issues with `label:auto-responded`
- Monitoring issue resolution time
- Collecting user feedback on response helpfulness
- Analyzing which topics are most frequently detected

## Maintenance

The workflow should be reviewed periodically to:
- Update documentation references as project evolves
- Add new topic detection for emerging common questions
- Improve keyword detection accuracy
- Update version numbers in examples
- Ensure links remain valid
