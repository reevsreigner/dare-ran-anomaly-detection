"""
Fix INT32 retransmission columns in problem Parquet files.
Reads each file, casts retransmission columns to float32, writes back in place.
Run from project root with venv activated:
    python scripts/fix_retx_dtype.py
"""

import pyarrow.parquet as pq
import pyarrow as pa
import pathlib

base = pathlib.Path('E:/dare_preprocessed/parquet')
files = list(base.rglob('*.parquet'))
print(f'Scanning {len(files)} files...')

# ── Find problem files ────────────────────────────────────────────────────────
problem_files = []
for i, f in enumerate(files):
    if i % 200 == 0:
        print(f'  Scan {i}/{len(files)}')
    schema = pq.read_schema(f)
    for field in schema:
        if 'retransmission' in field.name and field.type not in (pa.float32(), pa.float64()):
            problem_files.append(f)
            break

print(f'\nFound {len(problem_files)} files to fix\n')

# ── Fix each file ─────────────────────────────────────────────────────────────
fixed = 0
errors = []

for f in problem_files:
    try:
        # Read the table
        table = pq.read_table(f)

        # Cast all retransmission INT32 columns to float32
        new_columns = []
        new_fields  = []
        for i, field in enumerate(table.schema):
            col = table.column(i)
            if 'retransmission' in field.name and field.type not in (pa.float32(), pa.float64()):
                col   = col.cast(pa.float32())
                field = field.with_type(pa.float32())
            new_columns.append(col)
            new_fields.append(field)

        # Rebuild table with fixed schema
        new_schema = pa.schema(new_fields)
        fixed_table = pa.table(
            {field.name: new_columns[i] for i, field in enumerate(new_fields)},
            schema=new_schema
        )

        # Write back in place (same path, same compression)
        pq.write_table(
            fixed_table,
            str(f),
            compression='snappy'
        )

        fixed += 1
        print(f'  [{fixed}/{len(problem_files)}] Fixed: {f.name}')

    except Exception as e:
        errors.append((f, str(e)))
        print(f'  ERROR: {f.name} — {e}')

# ── Summary ───────────────────────────────────────────────────────────────────
print(f'\n{"="*60}')
print(f'DONE')
print(f'  Fixed : {fixed}')
print(f'  Errors: {len(errors)}')
if errors:
    print('  Error details:')
    for ef, msg in errors:
        print(f'    {ef.name}: {msg}')
print(f'{"="*60}')
print('\nNext step: re-upload fixed files to GCS')
print('  gsutil -m cp -r E:\\dare_preprocessed\\parquet gs://dare-raw-nist-anomaly/')
