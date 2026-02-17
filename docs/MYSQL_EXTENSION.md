# Ollama + MySQL extension

This repo includes the **Ollama + MySQL** extension. When you use `provider = "ollama_mysql"` in your `[ai.xxx]` section, the monitor will:

1. **Query your local MySQL** on each listing (similar items, price history).
2. **Inject that context** into the AI prompt so Ollama can compare.
3. **Include DB summary in notifications** (configurable: full, short, or none).

## Quick setup

1. **Install a MySQL driver** (optional; only needed if you use the MySQL block):

   ```bash
   pip install mysql-connector-python
   # or
   pip install PyMySQL
   ```

2. **Configure** in `~/.ai-marketplace-monitor/config.toml`:

```toml
[ai.ollama_local]
provider = "ollama_mysql"
base_url = "http://localhost:11434/v1"
model = "llama3.2"

[ai.ollama_local.mysql]
host = "localhost"
user = "marketplace"
password = "your_password"
database = "marketplace_db"
comparison_table = "listings"
max_rows = 10
output_format = "full"

[item.gopro]
search_phrases = "Go Pro Hero"
min_price = 100
max_price = 300
ai = ["ollama_local"]
```

See the [main README](https://github.com/BoPeng/ai-marketplace-monitor) and the example above for full config options. Use `comparison_table` for built-in “similar listings” by title/price, or `comparison_query` with placeholders `{title}`, `{price}`, `{location}`, `{item_name}` for a custom SQL query.
