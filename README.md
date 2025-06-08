# JSON to SQL Query Generator

This project provides a modular Python solution for converting nested JSON queries into ANSI SQL. It supports complex logical operations, event-based and user-based filters, and generates optimized SQL queries using Common Table Expressions (CTEs).

## Features

- Support for nested logical operators (AND/OR)
- Event-based and user-based filters
- Field-level conditions with various operators
- ANSI SQL compliant output
- Modular and extensible design
- Input validation using Pydantic models
- Clear separation of concerns

## Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

```python
from models import CompositeFilter
from sql_generator import SQLQueryGenerator

# Your JSON query
json_query = {
    "filter_operator": "or",
    "filters": [
        # ... your filters here
    ]
}

# Parse and generate SQL
filter_obj = CompositeFilter.model_validate(json_query)
generator = SQLQueryGenerator()
sql_query = generator.generate_sql(filter_obj)
```

## Example

See `examples/example.py` for a complete working example with the sample JSON query.

## Project Structure

- `models.py` — Pydantic models for query structure validation
- `sql_generator.py` — Core SQL generation logic
- `generators/` — Contains event and user SQL generators
- `operators/` — Contains SQL operator logic
- `examples/` — Contains example usage
- `requirements.txt` — Project dependencies

## Extending the Code

To add new operators or filter types:

1. Add new operators to `operators/sql_operator.py`
2. Create new filter classes in `models.py`
3. Add corresponding generator methods in `sql_generator.py`

## License

MIT 