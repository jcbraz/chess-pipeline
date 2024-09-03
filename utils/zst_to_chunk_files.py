import zstandard as zstd

def zst_to_chunk_files(file_name: str) -> None:
    with zstd.open(file_name, 'r') as f:
        current = ''
        for i, line in enumerate(f):
            if line.startswith('[Event'):
                if current != '':
                    text_file = open(f"pgn/out_{i}.pgn", "w")
                    text_file.write(current)
                    text_file.close()
                    current = ''
            current = current + line
            if i % 100_000 == 0:
                print(f'line {i:8d}: {line}')
        text_file = open(f"pgn/out_last.pgn", "w")
        text_file.write(current)
        text_file.close()
