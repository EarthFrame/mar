# LLM Tool Calling with pymar

`pymar` is designed to be highly compatible with Large Language Models (LLMs) that support tool calling (also known as function calling). The `pymar.tools` module provides a set of flat, well-documented functions that return structured data, making them ideal for integration into AI agents and RAG pipelines.

## Why use `pymar.tools`?

1.  **Descriptive Docstrings**: Each function includes a clear description of its purpose, arguments, and return values, which LLMs use to understand when to call the tool.
2.  **Structured Returns**: Functions return simple Python types (strings, lists, dictionaries) or Pydantic-validated data, ensuring that the LLM receives predictable and easy-to-parse information.
3.  **Simplified API**: The tools layer abstracts away the complexity of the C++ backend and the object-oriented `MarArchive` class, providing a functional interface that matches the mental model of most LLMs.

## Integration Examples

### OpenAI / Anthropic (Native Tool Calling)

You can easily convert `pymar.tools` into tool definitions for major LLM providers.

```python
import pymar.tools as mar_tools

# Example tool definition for OpenAI
tools = [
    {
        "type": "function",
        "function": {
            "name": "mar_search",
            "description": mar_tools.mar_search.__doc__,
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path to the .mar archive"},
                    "index_path": {"type": "string", "description": "Path to the .mai index file"},
                    "query": {"type": "string", "description": "Search query string"},
                    "topk": {"type": "integer", "description": "Number of results to return", "default": 5}
                },
                "required": ["path", "index_path", "query"]
            }
        }
    }
]
```

### LangChain Integration

If you are using LangChain, you can use the `@tool` decorator to wrap `pymar` functions.

```python
from langchain.tools import tool
import pymar.tools as mar_tools

@tool
def search_mar_archive(path: str, index_path: str, query: str, topk: int = 5):
    """Search a MAR archive using a sidecar index."""
    return mar_tools.mar_search(path, index_path, query, topk)

# The tool is now ready to be used by a LangChain agent
tools = [search_mar_archive]
```

## Best Practices for LLM Prompting

When providing these tools to an LLM, consider the following tips in your system prompt:

-   **Archive Discovery**: Encourage the LLM to use `mar_list` or `mar_header` first to understand the contents and structure of an archive before performing a search or retrieval.
-   **Index Awareness**: Remind the LLM that `mar_search` requires a sidecar index (`.mai`). If one doesn't exist, it may need to call `mar_index` first.
-   **Error Handling**: The tools will return clear error messages if a file is not found or an archive is corrupt. Instruct the LLM to report these issues to the user or attempt a fix (e.g., re-indexing).

## Available Tools

| Tool | Description |
| :--- | :--- |
| `mar_create` | Create a new MAR archive from files/directories. |
| `mar_index` | Build a sidecar index (MinHash, Vector, etc.) for an archive. |
| `mar_search` | Perform similarity or semantic search using an index. |
| `mar_list` | List all filenames in an archive. |
| `mar_get` | Retrieve the contents of a specific file. |
| `mar_validate` | Check the integrity and checksums of an archive. |
| `mar_header` | Get version and metadata information. |
| `mar_hash` | Compute a fast, deterministic hash of the archive. |
| `mar_version` | Get the version of the MAR format and tool. |
