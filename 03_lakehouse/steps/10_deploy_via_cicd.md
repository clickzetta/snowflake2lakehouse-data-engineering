# 10 — CI/CD Deployment

## Original (Snowflake)

`01_snowflake/steps/10_deploy_via_cicd.sql` demonstrates deploying the
`fahrenheit_to_celsius_udf` Python UDF via GitHub Actions + SnowCLI:

1. Modify `function.py` to use `scipy.constants.convert_temperature`
2. Add `scipy` to `requirements.txt`
3. Push to GitHub → GitHub Actions runs `.github/workflows/build_and_deploy.yaml`
4. SnowCLI packages and deploys the UDF to Snowflake automatically

## Lakehouse equivalent

Since we replaced the Python UDF with a SQL UDF (`05_udf.sql`), CI/CD
deployment is simpler — no packaging step needed.

### Option A: cz-cli in GitHub Actions

```yaml
# .github/workflows/deploy.yml
name: Deploy to Lakehouse
on:
  push:
    branches: [main]
    paths:
      - '03_lakehouse/steps/*.sql'
      - '03_lakehouse/steps/*.py'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install cz-cli
        run: npm install -g @clickzetta/cz-cli

      - name: Deploy UDFs
        env:
          CLICKZETTA_SERVICE:   ${{ secrets.CLICKZETTA_SERVICE }}
          CLICKZETTA_INSTANCE:  ${{ secrets.CLICKZETTA_INSTANCE }}
          CLICKZETTA_WORKSPACE: ${{ secrets.CLICKZETTA_WORKSPACE }}
          CLICKZETTA_USERNAME:  ${{ secrets.CLICKZETTA_USERNAME }}
          CLICKZETTA_PASSWORD:  ${{ secrets.CLICKZETTA_PASSWORD }}
        run: |
          cz-cli sql -f 03_lakehouse/steps/05_udf.sql \
            --service $CLICKZETTA_SERVICE \
            --instance $CLICKZETTA_INSTANCE \
            --workspace $CLICKZETTA_WORKSPACE \
            --username $CLICKZETTA_USERNAME \
            --password $CLICKZETTA_PASSWORD \
            --sync --write

      - name: Update task scripts
        run: |
          export $(cat 03_lakehouse/.env | xargs)
          bash 03_lakehouse/steps/08_orchestrate_tasks.sh
```

Store connection credentials as GitHub Actions secrets:
`CLICKZETTA_SERVICE`, `CLICKZETTA_INSTANCE`, `CLICKZETTA_WORKSPACE`,
`CLICKZETTA_USERNAME`, `CLICKZETTA_PASSWORD`.

### Option B: Studio DevOps (Git integration)

Lakehouse Studio supports connecting a Git repository directly.
Changes pushed to the configured branch are automatically synced
to Studio tasks. See [Studio DevOps documentation](https://docs.clickzetta.com).
