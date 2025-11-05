# Refactoring Plan: Eliminate Circular Imports & Simplify Architecture

**Created:** November 5, 2025  
**Status:** Proposed  
**Priority:** High (Blocking test execution)

---

## Executive Summary

The codebase has a circular dependency between `setup_orchestrator.py` and `logs.audit_logger.py` that prevents proper testing. Additionally, `setup_orchestrator.py` may be redundant since `main.py` (to be created) should serve as the ultimate orchestrator.

**Root Cause:**
```
logs/__init__.py → logs.audit_logger → setup.create_logs → setup/__init__.py → setup_orchestrator → logs.audit_logger
```

**Strategy:** 3-phase refactoring focusing on separation of concerns and eliminating redundancy.

---

## Phase 1: Separate ORM Models from Setup Logic ⭐ IMMEDIATE

### Objective
Break the circular dependency by moving ORM model definitions out of `setup/` into a dedicated `models/` package.

### Current Problem
- `logs.audit_logger` imports ORM models from `setup.create_logs`
- `setup_orchestrator` imports logger classes from `logs.audit_logger`
- Creates circular dependency when packages initialize

### Refactoring Steps

#### Step 1.1: Create `models/` Package Structure
```
models/
├── __init__.py              # Central export point for all models
├── logs_models.py           # ProcessLog, ConfigurationLog, ErrorLog, PerformanceMetrics, DataLineage
└── base.py                  # SQLAlchemy declarative base (if needed)
```

#### Step 1.2: Move ORM Models from `setup/create_logs.py`

**From:** `setup/create_logs.py` (lines ~50-250)  
**To:** `models/logs_models.py`

Move these ORM model classes:
- `ProcessLog`
- `ConfigurationLog`
- `ErrorLog`
- `PerformanceMetrics`
- `DataLineage`

**Keep in `setup/create_logs.py`:**
- `LoggingInfrastructure` class (setup/creation logic)
- Schema creation methods
- Index creation methods

#### Step 1.3: Update Imports

**Files to Update:**

1. **`logs/audit_logger.py`** (line 74)
   ```python
   # BEFORE:
   from setup.create_logs import (
       ConfigurationLog,
       DataLineage,
       ErrorLog,
       PerformanceMetrics,
       ProcessLog,
   )
   
   # AFTER:
   from models.logs_models import (
       ConfigurationLog,
       DataLineage,
       ErrorLog,
       PerformanceMetrics,
       ProcessLog,
   )
   ```

2. **`setup/create_logs.py`**
   ```python
   # Add at top:
   from models.logs_models import (
       ConfigurationLog,
       DataLineage,
       ErrorLog,
       PerformanceMetrics,
       ProcessLog,
   )
   ```

3. **Any other files importing these models** (search: `from setup.create_logs import`)

#### Step 1.4: Update `models/__init__.py`
```python
"""
========================================
ORM Models for Data Warehouse
========================================

Centralized SQLAlchemy ORM model definitions for the medallion architecture.

This package contains all database table models, separated from setup/creation logic
to prevent circular imports and improve maintainability.

Modules:
    logs_models: Audit logging and process tracking models
    schema_models: (Future) Bronze/Silver/Gold layer table models
"""

__version__ = "0.1.0"
__all__ = [
    # Logs schema models
    'ProcessLog',
    'ConfigurationLog',
    'ErrorLog',
    'PerformanceMetrics',
    'DataLineage',
]

from .logs_models import (
    ConfigurationLog,
    DataLineage,
    ErrorLog,
    PerformanceMetrics,
    ProcessLog,
)
```

**Expected Outcome:**
✅ `logs.audit_logger` no longer depends on `setup/` package  
✅ Circular import chain broken  
✅ Tests can import `logs.audit_logger` without triggering `setup_orchestrator`

---

## Phase 2: Simplify Package `__init__.py` Files 🎯 HIGH PRIORITY

### Objective
Remove eager imports from package initialization to prevent cascading import chains.

### Current Problem
- `logs/__init__.py` imports all submodules at package initialization
- `setup/__init__.py` imports all submodules including `setup_orchestrator`
- Any import of `logs` or `setup` triggers full module tree loading

### Refactoring Steps

#### Step 2.1: Make `logs/__init__.py` Lazy

**Current (lines 70-73):**
```python
from .audit_logger import ConfigurationLogger, ProcessLogger
from .data_lineage import LineageAnalyzer, LineageTracker
from .error_handler import ErrorLogger, ErrorRecovery
from .performance_monitor import MetricsCollector, PerformanceMonitor
```

**Refactored:**
```python
"""
=============================================================
Logging and monitoring infrastructure for the data warehouse.
=============================================================
[... keep docstring ...]
"""

__version__ = "0.1.0"
__all__ = [
    'ProcessLogger', 'ConfigurationLogger',
    'ErrorLogger', 'ErrorRecovery', 
    'PerformanceMonitor', 'MetricsCollector',
    'LineageTracker', 'LineageAnalyzer'
]

# Lazy imports using __getattr__ for Python 3.7+
def __getattr__(name):
    """Lazy-load submodules to avoid circular imports."""
    if name in __all__:
        if name in ('ProcessLogger', 'ConfigurationLogger'):
            from .audit_logger import ProcessLogger, ConfigurationLogger
            return ProcessLogger if name == 'ProcessLogger' else ConfigurationLogger
        elif name in ('ErrorLogger', 'ErrorRecovery'):
            from .error_handler import ErrorLogger, ErrorRecovery
            return ErrorLogger if name == 'ErrorLogger' else ErrorRecovery
        elif name in ('PerformanceMonitor', 'MetricsCollector'):
            from .performance_monitor import PerformanceMonitor, MetricsCollector
            return PerformanceMonitor if name == 'PerformanceMonitor' else MetricsCollector
        elif name in ('LineageTracker', 'LineageAnalyzer'):
            from .data_lineage import LineageTracker, LineageAnalyzer
            return LineageTracker if name == 'LineageTracker' else LineageAnalyzer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
```

#### Step 2.2: Make `setup/__init__.py` Lazy

**Current (lines 60):**
```python
from .create_database import DatabaseCreator
from .create_logs import LoggingInfrastructure
from .create_schemas import SchemaCreator
from .setup_orchestrator import SetupOrchestrator
```

**Refactored:**
```python
"""
========================================================
Setup package for medallion architecture data warehouse.
========================================================
[... keep docstring ...]
"""

__version__ = "0.1.0"
__author__ = "Laurent's Architecture Team"
__all__ = [
    'SetupOrchestrator',
    'DatabaseCreator',
    'SchemaCreator',
    'LoggingInfrastructure'
]

def __getattr__(name):
    """Lazy-load setup modules to avoid circular imports."""
    if name == 'DatabaseCreator':
        from .create_database import DatabaseCreator
        return DatabaseCreator
    elif name == 'SchemaCreator':
        from .create_schemas import SchemaCreator
        return SchemaCreator
    elif name == 'LoggingInfrastructure':
        from .create_logs import LoggingInfrastructure
        return LoggingInfrastructure
    elif name == 'SetupOrchestrator':
        from .setup_orchestrator import SetupOrchestrator
        return SetupOrchestrator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
```

**Expected Outcome:**
✅ Importing `logs` package doesn't automatically load all submodules  
✅ Importing `setup` package doesn't automatically load `setup_orchestrator`  
✅ Test imports only load what they need

---

## Phase 3: Deprecate/Refactor `setup_orchestrator.py` 🚀 ARCHITECTURAL

### Objective
Since `main.py` will serve as the ultimate orchestrator, simplify or deprecate `setup_orchestrator.py`.

### Current Role of `setup_orchestrator.py`
- Coordinates database creation, schema setup, and logging infrastructure
- Uses `ProcessLogger` and `ConfigurationLogger` for audit tracking
- Provides rollback capabilities

### Proposed Changes: Two Options

#### Option A: Convert to Simple Setup Script (Recommended)

**Rename:** `setup_orchestrator.py` → `setup_runner.py`

**Remove:**
- Process logging integration (audit tracking)
- Configuration change tracking
- Move orchestration logic to `main.py`

**Keep:**
- Simple sequential setup execution
- Error handling
- Rollback capabilities

**New `setup/setup_runner.py`:**
```python
"""
Simple setup runner for database initialization.
Orchestration moved to main.py for consistency.
"""

def run_database_setup(config=None):
    """
    Run database setup without audit logging.
    Audit logging will be added by main.py orchestrator.
    """
    from .create_database import DatabaseCreator
    from .create_schemas import SchemaCreator
    from .create_logs import LoggingInfrastructure
    
    results = {}
    
    # Step 1: Create database
    db_creator = DatabaseCreator(config)
    results['database'] = db_creator.create_database()
    
    # Step 2: Create schemas
    schema_creator = SchemaCreator(config)
    results['schemas'] = schema_creator.create_all_schemas()
    
    # Step 3: Create logging infrastructure
    logging_infra = LoggingInfrastructure(config)
    results['logging'] = logging_infra.setup_logging_infrastructure()
    
    return results
```

**Update `main.py` (to be created):**
```python
"""
Main orchestrator for SQL Data Warehouse Analytics Project.
Coordinates setup, ETL, and visualization.
"""

def main():
    from core.config import config
    from logs.audit_logger import ProcessLogger
    from setup.setup_runner import run_database_setup
    
    # Initialize audit logging
    process_logger = ProcessLogger(**config.get_db_params())
    
    # Start process tracking
    setup_id = process_logger.start_process(
        process_name='warehouse_initialization',
        process_description='Complete warehouse setup and initialization'
    )
    
    try:
        # Run setup
        setup_results = run_database_setup(config)
        
        # Run ETL (future)
        # Run visualizations (future)
        
        process_logger.end_process(setup_id, 'SUCCESS')
    except Exception as e:
        process_logger.end_process(setup_id, 'FAILED', error_message=str(e))
        raise
```

#### Option B: Deprecate Entirely

Mark `setup_orchestrator.py` as deprecated and move all orchestration to `main.py`.

**Add deprecation warning:**
```python
import warnings
warnings.warn(
    "setup_orchestrator.py is deprecated. Use main.py for orchestration.",
    DeprecationWarning,
    stacklevel=2
)
```

**Expected Outcome:**
✅ Clear separation: `setup/` does setup, `main.py` does orchestration  
✅ No circular dependency between `setup` and `logs`  
✅ Audit logging added by orchestrator, not by setup modules

---

## Phase 4: Update Tests 🧪 VALIDATION

### Objective
Update test suite to work with refactored architecture.

### Changes Required

#### Step 4.1: Update Test Imports

**`tests/tests_logs/test_audit_logger.py`:**
```python
# BEFORE:
from logs.audit_logger import ProcessLogger  # Triggers logs/__init__.py

# AFTER:
from logs.audit_logger import ProcessLogger  # No longer triggers full package
```

#### Step 4.2: Update Test Fixtures

**`tests/tests_logs/conftest.py`:**
```python
# Can now safely import without circular dependency
@pytest.fixture
def process_logger_factory():
    """Factory for ProcessLogger instances."""
    def factory(**overrides):
        from logs.audit_logger import ProcessLogger  # Safe now
        params = dict(
            host="localhost",
            port=5432,
            user="postgres",
            password="secret",
            database="warehouse"
        )
        params.update(overrides)
        return ProcessLogger(**params)
    return factory
```

#### Step 4.3: Add Integration Tests

**`tests/tests_setup/test_setup_runner.py`** (if using Option A):
```python
"""Tests for setup_runner.py (simplified orchestrator)."""

def test_run_database_setup_success():
    """Test that setup runs all components in order."""
    from setup.setup_runner import run_database_setup
    # ... test implementation
```

**Expected Outcome:**
✅ All 30 tests in `test_audit_logger.py` pass  
✅ No circular import errors  
✅ Tests can be run in isolation

---

## Implementation Order & Timeline

### Week 1: Phase 1 (Break Circular Dependency)
- [ ] Day 1-2: Create `models/` package and move ORM models
- [ ] Day 3: Update all imports across codebase
- [ ] Day 4: Run full test suite, fix any import issues
- [ ] Day 5: Code review and validation

### Week 2: Phase 2 (Lazy Package Initialization)
- [ ] Day 1: Implement lazy imports in `logs/__init__.py`
- [ ] Day 2: Implement lazy imports in `setup/__init__.py`
- [ ] Day 3-4: Test all import paths, ensure backward compatibility
- [ ] Day 5: Documentation updates

### Week 3: Phase 3 (Refactor Orchestrator)
- [ ] Day 1-2: Decide on Option A vs Option B
- [ ] Day 3: Implement chosen option (setup_runner or deprecation)
- [ ] Day 4: Create `main.py` skeleton with orchestration
- [ ] Day 5: Integration testing

### Week 4: Phase 4 (Update Tests & Validation)
- [ ] Day 1-2: Update all test imports and fixtures
- [ ] Day 3: Run complete test suite (setup + logs)
- [ ] Day 4: Add new integration tests for main.py
- [ ] Day 5: Final validation and documentation

---

## Migration Path for Existing Code

### Backward Compatibility

**During transition period, support both import styles:**

```python
# Old style (deprecated but still works)
from setup.create_logs import ProcessLog

# New style (preferred)
from models.logs_models import ProcessLog
```

**Add deprecation warnings:**
```python
# In setup/create_logs.py
import warnings
from models.logs_models import ProcessLog  # noqa

warnings.warn(
    "Importing ORM models from setup.create_logs is deprecated. "
    "Use 'from models.logs_models import ProcessLog' instead.",
    DeprecationWarning,
    stacklevel=2
)
```

### Testing Strategy

1. **Unit Tests:** Run after each phase to catch regressions
2. **Integration Tests:** Full setup → ETL flow (after Phase 3)
3. **Import Tests:** Verify no circular imports at each phase
4. **Performance Tests:** Ensure lazy loading doesn't impact runtime

---

## Success Criteria

### Phase 1 Success
- ✅ `pytest tests/tests_logs/test_audit_logger.py` passes all 30 tests
- ✅ No circular import errors
- ✅ `models/` package contains all ORM models

### Phase 2 Success
- ✅ Can import individual modules without loading entire package
- ✅ All existing code still works (backward compatible)
- ✅ Test execution time improves (lazy loading)

### Phase 3 Success
- ✅ Clear separation: setup vs orchestration
- ✅ `main.py` controls all workflow
- ✅ No dependencies from `setup/` to `logs/`

### Phase 4 Success
- ✅ All test suites pass (setup + logs + integration)
- ✅ Test coverage ≥ 85%
- ✅ No import errors in any configuration

---

## Risk Mitigation

### Risk 1: Breaking Changes
**Mitigation:** Maintain backward compatibility for 1-2 releases, add deprecation warnings

### Risk 2: Performance Impact (Lazy Loading)
**Mitigation:** Benchmark import times, optimize critical paths if needed

### Risk 3: Incomplete Migration
**Mitigation:** Use automated search/replace, comprehensive test coverage

### Risk 4: Missed Import Dependencies
**Mitigation:** Static analysis tools (pylint, mypy), comprehensive test suite

---

## Alternatives Considered

### Alternative 1: Keep Circular Import, Use Lazy Imports Everywhere
**Rejected:** Band-aid solution, doesn't address root architectural issue

### Alternative 2: Merge `setup/` and `logs/` Packages
**Rejected:** Violates separation of concerns, makes codebase harder to maintain

### Alternative 3: Dependency Injection for All Logger Usage
**Rejected:** Over-engineering for current scope, can be added later if needed

---

## Follow-up Actions

### After Refactoring Complete
1. Update documentation (README, docstrings)
2. Create architecture diagram showing new structure
3. Add CI/CD checks for circular imports
4. Consider adding `import-linter` to prevent future circular dependencies

### Future Enhancements
1. Add Bronze/Silver/Gold ORM models to `models/schema_models.py`
2. Create `orchestrators/` package for different workflow orchestrators
3. Implement dependency injection for better testability

---

## Questions for Decision

1. **Phase 3 Option:** Prefer Option A (setup_runner.py) or Option B (full deprecation)?
   - **Recommendation:** Option A for gradual transition

2. **Timeline:** Acceptable to spend 3-4 weeks on this refactoring?
   - **Note:** Can be done incrementally alongside feature development

3. **Breaking Changes:** Acceptable to break backward compatibility in next major version?
   - **Recommendation:** Maintain compatibility with deprecation warnings for 2 releases

---

## Appendix: File-by-File Change Summary

### New Files
- `models/__init__.py`
- `models/logs_models.py`
- `models/base.py` (optional)
- `setup/setup_runner.py` (if Option A)
- `main.py` (orchestrator)
- `REFACTORING_PLAN.md` (this file)

### Modified Files
- `logs/audit_logger.py` - Update imports from setup → models
- `logs/__init__.py` - Add lazy loading with __getattr__
- `setup/__init__.py` - Add lazy loading with __getattr__
- `setup/create_logs.py` - Remove ORM models, keep setup logic
- `setup/setup_orchestrator.py` - Deprecate or refactor to setup_runner
- `tests/tests_logs/conftest.py` - Simplify fixtures
- `tests/tests_logs/test_audit_logger.py` - Update imports
- All files importing from `setup.create_logs` - Update to `models.logs_models`

### Deprecated Files
- `setup/setup_orchestrator.py` (if Option B chosen)

---

**END OF REFACTORING PLAN**
