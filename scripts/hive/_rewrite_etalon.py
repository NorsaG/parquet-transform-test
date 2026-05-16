import re
from pathlib import Path
p = Path(r"C:\Users\norsa\IdeaProjects\parquet-transform-test\scripts\hive\select_problematic_queries_clear.sql")
s = p.read_text(encoding="utf-8")
pat = re.compile(r"WITH etalon\(case_id, etalon_value\) AS \(\n  SELECT \* FROM VALUES\n(?P<body>.*?)\n\)\nSELECT", re.S)
def conv(m):
    body = m.group("body")
    rows = []
    for line in body.splitlines():
        line = line.strip().rstrip(",")
        if not line:
            continue
        mm = re.match(r"\('((?:''|[^'])*)',\s*(NULL|'(?:''|[^'])*')\)", line)
        if not mm:
            continue
        rows.append(("'" + mm.group(1) + "'", mm.group(2)))
    parts = [f"    {a}, {b}" for a, b in rows]
    return (
        "WITH etalon AS (\n"
        f"  SELECT stack({len(rows)},\n"
        + ",\n".join(parts)
        + "\n  ) AS (case_id, etalon_value)\n)\nSELECT"
    )
ns = pat.sub(conv, s)
p.write_text(ns, encoding="utf-8")
print("converted")
