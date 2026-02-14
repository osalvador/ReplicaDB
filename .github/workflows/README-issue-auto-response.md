# Issue Auto-Response Agent

## Overview

This GitHub Actions workflow automatically responds to newly opened issues with helpful, context-aware information based on the issue content and project documentation. **All responses are in English** and issues are **automatically labeled** based on their type.

## Purpose

The auto-response agent:
- Provides immediate assistance to users opening issues
- Analyzes issue content to detect common topics and questions
- Generates relevant responses with links to documentation and resources
- **Automatically labels issues by type** (bug, question, enhancement, documentation)
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
3. **Analyze Issue**: Examines the issue title and body for:
   - **Issue type** (bug, question, enhancement, documentation)
   - **Topic keywords** related to installation, databases, performance, modes, configuration, troubleshooting, docker
4. **Generate Response**: Creates a contextual response in **English** that includes:
   - Relevant information based on detected topics
   - Code examples and command snippets
   - Links to detailed documentation
   - Troubleshooting guidance
   - Request for additional details if needed
5. **Post Comment**: Automatically posts the generated response as a comment on the issue
6. **Add Labels**: Tags the issue with:
   - `auto-responded` (to track automated responses)
   - Type label: `bug`, `question`, `enhancement`, or `documentation`

## Issue Type Detection

The agent automatically detects the issue type based on keywords:

### Bug 🐛
**Keywords**: error, exception, fail, failed, crash, broken, bug, defect, incorrect, wrong
**Label**: `bug`

### Question ❓
**Keywords**: how to, how do, how can, what is, why does, when should, where, question, contains `?`
**Label**: `question`

### Enhancement ✨
**Keywords**: feature, enhancement, improvement, add support, new feature, would be nice, could you add, feature request
**Label**: `enhancement`

### Documentation 📚
**Keywords**: documentation, docs, readme, guide, tutorial, example, document
**Label**: `documentation`

**Note**: If an issue matches multiple types, all relevant labels are applied. If no type is detected, it defaults to `question`.

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

When a user opens an issue with keywords like "error" and "postgres", the agent will:
1. Detect type: **bug** (due to "error" keyword)
2. Detect topics: **database**, **troubleshooting**
3. Apply labels: `auto-responded`, `bug`
4. Post response with:

```markdown
👋 Hello @username! Thank you for opening this issue.

🤖 **Automated Initial Response**

### 🗄️ Supported Databases
ReplicaDB supports a wide range of databases:
- **Relational:** Oracle, PostgreSQL, MySQL/MariaDB...
[database information...]

### 🔍 Troubleshooting
If you encounter errors:
1. **Verify connectivity:** Ensure both databases are accessible
[troubleshooting steps...]

### 📚 Additional Resources
- 📖 [Complete documentation](...)
[additional links...]

---
*This response was automatically generated based on detected topics: database, troubleshooting.*

If you need more specific help, please provide:
- ReplicaDB version you are using
- Source and sink databases (type and version)
- Command or configuration file you are running
- Complete error messages if any

A human maintainer will review your issue soon! 🙂
```

## Permissions

The workflow requires:
- `issues: write` - To post comments and add labels
- `contents: read` - To read documentation files

## Configuration

The workflow is configured in `.github/workflows/issue-auto-response.yml` and runs automatically. No additional configuration is needed.

## Customization

To modify the response logic:

1. **Change issue type detection**: Edit the keyword lists in the issue type detection section
2. **Add new labels**: Add additional label conditions in the labeling logic
3. **Update response templates**: Modify response text for each topic section
4. **Add new topic detection**: Create new topic sections with keyword matching

## Labels

The workflow automatically applies the following labels:
- `auto-responded` - Always added to track automated responses
- `bug` - For error reports and defects
- `question` - For how-to questions and general inquiries
- `enhancement` - For feature requests and improvements
- `documentation` - For documentation-related issues

**Note**: These labels will be created automatically if they don't exist in the repository.

## Benefits

- **Immediate Response**: Users get instant feedback when opening issues
- **Consistent Information**: Ensures all users receive accurate, up-to-date information in English
- **Reduced Maintainer Load**: Handles common questions automatically
- **Improved Triage**: Pre-categorizes issues with type-based labels
- **Better User Experience**: Users feel acknowledged and receive guidance immediately
- **Multilingual Support**: Detects keywords in multiple languages (English, Spanish)

## Limitations

- The agent provides general guidance based on keyword matching
- Complex or unique issues will still require human maintainer review
- Responses are in English regardless of issue language
- Cannot access external resources or perform database testing

## Future Enhancements

Potential improvements:
- Integration with AI services (OpenAI, Anthropic) for more sophisticated responses
- Dynamic response language based on issue language detection
- Code analysis for issues mentioning specific files or functions
- Integration with GitHub Discussions for FAQ responses
- Analytics on common issue topics to improve documentation

## Monitoring

Track the effectiveness of auto-responses by:
- Filtering issues with `label:auto-responded`
- Monitoring issue resolution time by type
- Collecting user feedback on response helpfulness
- Analyzing which topics and types are most frequently detected

## Maintenance

The workflow should be reviewed periodically to:
- Update documentation references as project evolves
- Add new topic and type detection for emerging common questions
- Improve keyword detection accuracy
- Update version numbers in examples
- Ensure links remain valid
