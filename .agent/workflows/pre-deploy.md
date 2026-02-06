---
description: Run code quality checks before deployment
---

# Pre-Deployment Code Quality Checks

Follow these steps before deploying to production:

## 1. Run Ruff Critical Checks

Check for critical bugs (undefined variables, syntax errors, etc.)

```bash
python -m ruff check child_bot.py database.py main.py --select F
```

**Expected:** 0-1 errors (max 1 unused variable is acceptable)
**If errors found:** Fix before proceeding

---

## 2. Python Syntax Check

Verify all Python files compile without errors

```bash
python -m py_compile child_bot.py
python -m py_compile database.py
python -m py_compile main.py
```

**Expected:** All files pass
**If errors found:** Fix syntax errors

---

## 3. Git Status Check

Ensure all changes are committed

```bash
git status
```

**Expected:** "working tree clean" or only untracked files
**If modified files:** Commit them first

---

## 4. Push to GitHub

// turbo

```bash
git push origin main
```

**Expected:** "Everything up-to-date" or successful push

---

## 5. Deploy on Coolify

1. Login to Coolify dashboard
2. Navigate to bot-platform deployment
3. Click "Redeploy" or "Pull & Rebuild"
4. Wait for deployment to complete

---

## 6. Verify Deployment

Check latest logs after deployment:

```bash
# Download logs from Coolify and check for:
# - ✅ Mother Bot Ready
# - ✅ X/X Child Bots Hooked
# - ❌ No Python errors on startup
```

---

## Optional: Full Ruff Audit

Run complete code quality check (includes style issues)

```bash
python -m ruff check . --select F,E,W --output-format=concise
```

**Note:** May show 1000+ warnings (mostly style) - focus on F-level only
