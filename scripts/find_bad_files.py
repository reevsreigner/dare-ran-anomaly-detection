import pyarrow.parquet as pq
import pyarrow as pa
import pathlib

base = pathlib.Path('E:/dare_preprocessed/parquet')
files = list(base.rglob('*.parquet'))
print(f'Scanning {len(files)} files...')

problem_files = []
for i, f in enumerate(files):
    if i % 200 == 0:
        print(f'  {i}/{len(files)}')
    schema = pq.read_schema(f)
    for field in schema:
        if 'retransmission' in field.name and field.type not in (pa.float32(), pa.float64()):
            problem_files.append(f)
            break

print(f'\nProblem files: {len(problem_files)}')
for pf in problem_files[:10]:
    print(f'  {pf}')
