# PileupGenerator

Standalone `pileupconf.json` generator that queries DBS, MSPileup, and Rucio to
produce the same pileup configuration JSON that WMCore's `PileupFetcher` builds
-- without requiring WMCore step/task objects.

## Prerequisites

- Python 3
- A valid CMS grid proxy (the script talks to authenticated CMS services)
- WMCore libraries on `PYTHONPATH` (for `DBSReader`, `Rucio`, and `MSUtils`)

## Quick start

```bash
# Minimal -- just a dataset, defaults to the prod instance
python generate_pileupconf.py \
    --dataset /MinBias_TuneCP5_5TeV-pythia8/RunIISummer20UL17pp5TeVGS-106X_mc2017_realistic_forppRef5TeV_v3-v1/GEN-SIM
```

The output is written to `pileupconf.json` in the current directory.

## Usage examples

### Specify the instance and output path

```bash
python generate_pileupconf.py \
    --dataset /MinBias_TuneCP5_5TeV-pythia8/.../GEN-SIM \
    --instance testbed \
    --output my_pileupconf.json
```

### Override individual service URLs

Per-URL flags take precedence over the `--instance` preset:

```bash
python generate_pileupconf.py \
    --dataset /MinBias_TuneCP5_5TeV-pythia8/.../GEN-SIM \
    --instance prod \
    --dbs-url https://custom-dbs/endpoint
```

### Auto-extract datasets from a request.json

The script scans `StepN` / `TaskN` entries for `MCPileup` and `DataPileup`
fields, so you don't have to specify datasets manually:

```bash
python generate_pileupconf.py \
    --request-json /path/to/request.json
```

### Set the pileup type

When using `--dataset`, the pileup type defaults to `mc`. Override it with
`--pileup-type`:

```bash
python generate_pileupconf.py \
    --dataset /SomeDataPileup/.../RAW \
    --pileup-type data
```

### Verbose logging

Add `-v` for DEBUG-level output showing every service call and block-level
detail:

```bash
python generate_pileupconf.py \
    --dataset /MinBias_.../GEN-SIM -v
```

## Instance presets

The `--instance` flag sets DBS, MSPileup, and Rucio URLs consistently:

| Instance  | DBS                                                        | MSPileup                                                | Rucio auth / host                                        |
| --------- | ---------------------------------------------------------- | ------------------------------------------------------- | -------------------------------------------------------- |
| `prod`    | `https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader`   | `https://cmsweb.cern.ch/ms-pileup/data/pileup`         | `cms-rucio-auth.cern.ch` / `cms-rucio.cern.ch`          |
| `testbed` | `https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader` | `https://cmsweb-testbed.cern.ch/ms-pileup/data/pileup` | `cms-rucio-auth-int.cern.ch` / `cms-rucio-int.cern.ch`  |

## Output format

The generated JSON has the structure:

```json
{
  "<pileup_type>": {
    "<block_name>": {
      "FileList": ["lfn1", "lfn2", "..."],
      "NumberOfEvents": 12345,
      "PhEDExNodeNames": ["RSE1", "RSE2"]
    }
  }
}
```

## How it works

1. **Resolve URLs** from `--instance` and any per-URL overrides.
2. **Query DBS** (`DBSReader.getFileListByDataset`) for files, blocks, and
   event counts.
3. **Query MSPileup** (`getPileupDocs`) for `currentRSEs`, `customName`, and
   `containerFraction`.
4. **Query Rucio** (`Rucio.getBlocksInContainer`) to filter out blocks not
   registered in Rucio and set `PhEDExNodeNames` from the MSPileup
   `currentRSEs`.
5. **Write JSON** to the output file.

## CLI reference

```
usage: generate_pileupconf.py [-h] (--dataset DATASET | --request-json REQUEST_JSON)
                              [--instance {prod,testbed}]
                              [--dbs-url DBS_URL] [--mspileup-url MSPILEUP_URL]
                              [--rucio-auth-url RUCIO_AUTH_URL]
                              [--rucio-host-url RUCIO_HOST_URL]
                              [--pileup-type PILEUP_TYPE]
                              [--output OUTPUT] [--verbose]
```

| Flag                | Default            | Description                                              |
| ------------------- | ------------------ | -------------------------------------------------------- |
| `--dataset`         | *(required\*)*     | Pileup dataset name                                      |
| `--request-json`    | *(required\*)*     | Path to a ReqMgr-style request.json                      |
| `--instance`        | `prod`             | Service instance preset (`prod` or `testbed`)            |
| `--dbs-url`         | *(from instance)*  | Override DBS URL                                         |
| `--mspileup-url`    | *(from instance)*  | Override MSPileup URL                                    |
| `--rucio-auth-url`  | *(from instance)*  | Override Rucio auth URL                                  |
| `--rucio-host-url`  | *(from instance)*  | Override Rucio host URL                                  |
| `--pileup-type`     | `mc`               | Pileup type key (only with `--dataset`)                  |
| `--output`, `-o`    | `pileupconf.json`  | Output file path                                         |
| `--verbose`, `-v`   | off                | Enable DEBUG-level logging                               |

\* One of `--dataset` or `--request-json` is required (mutually exclusive).
