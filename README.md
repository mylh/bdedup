# bdedup

**Efficient command-line deduplication tool that uses a Bloom filter for high-performance duplicate detection in large datasets or streams.**

- Scales to huge datasets with low memory usage
- Supports persistent, gzipped Bloom filter state on disk
- Configurable expected dataset size and false positive rate
- Processes input in parallel for maximum speed (if reading from a file)
- Input/output via files or standard streams
- Can output only new or only previously-seen items

## Installation

```
go get github.com/mylh/bdedup
cd $GOPATH/src/github.com/mylh/bdedup
go build
```
The executable will be `bdedup` (or `bdedup.exe` on Windows).

---

## Usage

```
bdedup [options]
```

### Options

| Option        | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| `-input`      | Input file (default: stdin)                                                 |
| `-output`     | Output file (default: stdout)                                               |
| `-state`      | Bloom filter state file (default: bloom.gz)                                 |
| `-n`          | Expected number of distinct values (default: 1000000)                       |
| `-p`          | False positive probability (default: 0.01, i.e., 1%)                        |
| `-seen`       | Output only previously seen items (default: output only new items)          |
| `-concurrency`| Number of workers when processing files (default: number of CPU cores)      |
| `-h, -?, --help` | Show help and usage information                                          |

---

## Examples

### 1. Remove duplicates from a file and save result to another file

```sh
bdedup -input emails.txt -output unique-emails.txt -n 5000000 -p 0.001
```

### 2. Remove duplicates from a stream

```sh
cat log.txt | bdedup -n 100000 -p 0.01 > unique-log.txt
```

### 3. Output only lines that have previously been seen

```sh
cat ids.txt | bdedup -n 1000000 -p 0.01 -seen > duplicates.txt
```

### 4. Use and persist a shared Bloom filter state (across multiple runs/sessions)

```sh
bdedup -input newdata.csv -output deduped.csv -state myfilter.gz
```
Next time you run:
```sh
bdedup -input otherdata.csv -output deduped2.csv -state myfilter.gz
```

### 5. Process huge datasets in parallel

```sh
bdedup -input hugefile.txt -output unique.txt -n 20000000 -p 0.001 -concurrency 8
```

---

## How It Works

`bdedup` uses a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) from [github.com/AndreasBriese/bbloom](https://github.com/AndreasBriese/bbloom). A Bloom filter is a probabilistic set with a configurable false-positive rate and tiny memory use compared to full in-memory deduplication.

- For each line, the filter is queried and then updated.
- By default, only lines not previously seen are output.
- Use `-seen` to output only seen (duplicate) lines.
- Bloom filter state can be persisted and reused between runs.

---

## Notes & Caveats

- False positives are possible: a line might be incorrectly considered a duplicate due to the probabilistic nature. Tune `-p` (false positive probability) and `-n` (expected dataset size) for your needs.
- The filter is not reset on each run if the same `-state` file is used. The deduplication state persists.
- Input/output order may not be preserved when using file and parallel processing.
- Persistent state format is gzipped JSON, compatible with `bbloom`.

---

## License

MIT License

---

## Credits

Uses [github.com/AndreasBriese/bbloom](https://github.com/AndreasBriese/bbloom).

---

## Author

Maintained by [mylh](https://github.com/mylh)

---
