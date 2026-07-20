# Code Churn in Vulnerability and Bug Commits: An Empirical Analysis

## Quick Start (Recommended)

The submitted project already includes:

- All processed output files (CSV files)
- Jupyter notebooks containing the analyses and generated results
- Figures and statistical outputs used in the thesis

It is recommended to first copy the project from the USB drive to your local machine before running it. This avoids the slower read/write speeds of the USB drive and provides sufficient disk space should you wish to clone the source repositories.

### 1. Install Python

This project was developed and tested using **Python 3.14.4**. While it may work with earlier Python 3 versions, using the same version is recommended for maximum compatibility.

### 2. Create a Virtual Environment

Create a Python virtual environment named `venv`:

```bash
python -m venv venv
```

### 3. Activate the Virtual Environment

**Windows**

```bash
venv\Scripts\activate
```

**macOS / Linux**

```bash
source venv/bin/activate
```

### 4. Install the Required Packages

```bash
pip install -r requirements.txt
```

You can now open the Jupyter notebooks to inspect or reproduce the analyses presented in the thesis.

---

## Reproducing the Analysis from Scratch (Optional)

The following steps are only required if you wish to rerun the complete analysis from the original repositories.

### 1. Create a virtual environment

```bash
python -m venv venv
```

### 2. Activate the virtual environment

**Windows**

```bash
venv\Scripts\activate
```

**macOS / Linux**

```bash
source venv/bin/activate
```

### 3. Clone the required repositories

```bash
python scripts/clone_repos.py
```

## Running the Diffstat Pipeline

The scripts in `scripts/diffstat/` extract code churn metrics using the `diffstat` tool. Each script processes a specific dataset and produces the corresponding output CSV files used throughout the analysis.

### Installing Diffstat

The `diffstat` utility must be installed separately and be available on your system `PATH`.

**Ubuntu / Debian**

```bash
sudo apt install diffstat
```

**macOS (Homebrew)**

```bash
brew install diffstat
```

**Windows**

Download and install `diffstat` from a suitable distribution (e.g., via MSYS2 or Cygwin), then ensure the executable is available on your system `PATH`.

You can verify the installation by running:

```bash
diffstat --version
```

If the command prints the installed version, the scripts will be able to invoke `diffstat` successfully.

Run the scripts from the project root using Python's module syntax:

### DS_APACHE (Java Bug Dataset)

```bash
python -m scripts.diffstat.extract_churn_ds_apache
```

### ICVul (C/C++ Vulnerability Dataset)

```bash
python -m scripts.diffstat.extract_churn_icvul
```

**Multiprocessing version (recommended for the full dataset):**

```bash
python -m scripts.diffstat.extract_churn_icvul_multiprocessing
```

### Linux / KFC (C/C++ Bug Dataset)

```bash
python -m scripts.diffstat.extract_churn_linux
```

**Multiprocessing version (recommended for the full dataset):**

The multiprocessing implementation processes the dataset in chunks.

Example:

```bash
python -m scripts.diffstat.extract_churn_linux_multiprocessing --chunk 0 --num-chunks 8
```

Run one instance for each chunk (`0` to `7` when using `--num-chunks 8`).

### TOSEM / Hinrichs et al. (Java Vulnerability Dataset)

```bash
python -m scripts.diffstat.extract_churn_tosem
```

> **Note:** The multiprocessing versions are recommended for the larger C/C++ datasets (ICVul and Linux/KFC) as they significantly reduce the execution time.

## Running the String Matching Pipeline

The scripts in `scripts/string_matching/` extract code churn metrics using the custom string-matching algorithm developed for this study. Unlike `diffstat`, this approach distinguishes modified lines from pure additions and deletions by comparing the textual similarity of changed lines within each diff hunk.

Run the scripts from the project root using Python's module syntax:

### DS_APACHE (Java Bug Dataset)

```bash
python -m scripts.string_matching.extract_churn_ds_apache
```

### ICVul (C/C++ Vulnerability Dataset)

```bash
python -m scripts.string_matching.extract_churn_icvul
```

**Multiprocessing version (recommended for the full dataset):**

```bash
python -m scripts.string_matching.extract_churn_icvul_multiprocessing
```

### Linux / KFC (C/C++ Bug Dataset)

```bash
python -m scripts.string_matching.extract_churn_linux
```

**Multiprocessing version (recommended for the full dataset):**

```bash
python -m scripts.string_matching.extract_churn_linux_multiprocessing --chunk 0 --num-chunks 8
```

### Hinrichs et al. (Java Vulnerability Dataset)

```bash
python -m scripts.string_matching.extract_churn_tosem
```

> **Note:** The linux dataset multiprocessing scripts generate one output file per chunk. After all chunks have completed, the resulting CSV files can be easily merged into a single dataset using a simple Python script or directly within a Jupyter notebook before continuing with the statistical analysis.
