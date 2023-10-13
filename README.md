# EV Price Tracker - Cache

## Overview
This repo contains code to query a PostgreSQL database and updating Redis to accelerate backend performance. 
The code is triggered to run daily for regular updates.

## Technologies
- Cloud Function
   - Execute code using serverless compute
- Cloud Scheduler
   - Schedule execution of code
- Redis
   - Short term data retention for serving to backend
- PostgreSQL
   - Long term data retention for historical data

## Contributing

### General Guidelines
Please take a look at the following guides on writing code:
- [PEP 8 Style Guide](https://www.python.org/dev/peps/pep-0008/) for Python

### Set Up Development Environment
1. Clone and navigate to the repository
```shell
cd ~/GitHub/issaloo
git clone git@github.com:issaloo/ev-price-tracker-scraper.git
cd ev-price-tracker-scraper
```

2. Install pdm globally
```shell
pip install pdm
```

3. Install general & development packages with pdm
```shell
pdm install --dev
```
> :information_source: This will install packages [pre-commit](https://pre-commit.com/), [commitizen](https://commitizen-tools.github.io/commitizen/), and [gitlint](https://jorisroovers.com/gitlint/latest/)

(Optional) Install only the general packages
```shell
pdm install
```

3. Activate the virtual environment
```shell
eval $(pdm venv activate)
```
> :information_source: Virtual environment will use the same python version as the system

(Optional) Deactivate the virtual environment
```shell
deactivate
```

### Set Up Standardized Version Control

1. Automate scripts (i.e., linting and autoformatting)
   ```shell
   pre-commit install
   ```
   
2. Enforce template at commit with pre-commit
   ```shell
   gitlint install-hook
   ```

### Test It Out

1. **Check if `commitizen` is working**
   - :mag_right: Try using commitizen in command line 
      1. Add files to staging
      2. Run commitizen
         ```shell
         git cz c
         ```
         Or, if possible
         ```shell
         cz c
         ```
   - :white_check_mark: You should get structured commits

2. **Check if `gitlint` is working**
   - :mag_right: Try writing a bad commit
     1. Add files to staging
     2. Write a bad commit (e.g., `git commit -m 'WIP: baD commit'`)
   - :white_check_mark: You should get a question on whether to continue the commit.

3. **Check if `pre-commit` is working**
   - :mag_right: 
     1. Add files to staging, where at least one python file is not formatted well
     2. Run commitizen
         ```shell
         git cz c
         ```
         Or, if possible
         ```shell
         cz c
         ```
   - :white_check_mark: You should get automatic fixes to poorly formatted python files with some errors

   > :information_source:  Ctrl+C to exit commit template
