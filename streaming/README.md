# Spark streaming

### TODO

- create new `template_access_log.txt` file (from `access_log.txt) with small batches of logs and without timestamps
- create (bash?) script to copy/paste `template_access_log.txt` file from `streaming` to `streaming/logs` folder with name like `<current_timestap>_access_log.txt` every second to simulate arriving logs
- rewrite `structured-streaming.py` to parse logs and use `windows` to show most visited URL