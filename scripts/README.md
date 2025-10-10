# Scripts

Manual testing and debugging scripts for the worker pipeline.

## Available Scripts

### `test_extract_media.py`

Test the `enhance_repo_media` functionality for a specific repository.

**Usage:**

```bash
python scripts/test_extract_media.py owner/repo
```

**Example:**

```bash
python scripts/test_extract_media.py facebook/react
```

**Features:**

-   Shows README preview
-   Lists all markdown images found
-   Shows which images are kept/filtered and why
-   Displays normalized URLs
-   Optionally writes to database

## Adding New Scripts

When adding manual test scripts here:

1. Make them standalone and easy to run
2. Add clear usage documentation
3. Include verbose debug output
4. Ask before modifying database
5. Update this README
