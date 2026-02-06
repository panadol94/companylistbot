---
description: Quick Ruff check for critical bugs
---

# Quick Bug Check with Ruff

Run Ruff to detect critical bugs before committing:

## Critical Bugs Only (Fast)

// turbo

```bash
python -m ruff check child_bot.py database.py main.py --select F
```

✅ **Pass:** 0-1 errors (1 unused variable acceptable)
❌ **Fail:** Fix bugs before commit

---

## Auto-Fix Safe Issues

```bash
python -m ruff check . --select F401 --fix
```

This removes unused imports automatically.

---

## What Ruff Catches

- ❌ Undefined variables
- ❌ Unused imports
- ❌ Syntax errors
- ❌ Unreachable code
- ❌ Redefined functions
