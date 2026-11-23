# Phase 1 Snapshots

Snapshot of the final cleaned NYC dataset
<img width="1547" height="204" alt="Screenshot 2026-10-20 at 5 31 31 PM" src="https://github.com/user-attachments/assets/9f3186ce-fa78-47f5-b7e4-355ff1386d9a" />


Snapshot of the final cleaned LA dataset
<img width="1542" height="203" alt="Screenshot 2026-10-20 at 5 32 11 PM" src="https://github.com/user-attachments/assets/a5a2d62a-3064-4b1a-a679-7f0899c0880d" />

# Phase 3: PostgreSQL and WebUI Setup

This project uses PostgreSQL in Docker Desktop and a Streamlit UI. Run the commands below in the Ubuntu 24.04 WSL terminal from the `CS236-Course-Project` directory.

## 1. Start PostgreSQL

1. Start Docker Desktop in Windows and wait until it is fully active.
2. In Docker Desktop, open **Settings > Resources > WSL Integration** and enable integration for **Ubuntu-24.04**.
3. Please run version Ubuntu 24.04 using a WSL. 
4. In your WSL, copy `.env.example` to `.env` and replace DB_PASSWORD with your own local password. `.env` should not be committed to Git. 

   ```bash
   cp -n .env.example .env
   nano .env
   ```

4. Start the PostgreSQL container:

   ```bash
   sudo docker compose up -d postgres
   sudo docker compose ps
   ```

In this setup, Docker socket access from WSL requires `sudo`. Please use `sudo` for Docker Compose commands if you see a permission-denied error.

## 2. Populate the three tables

```bash
source ~/.venvs/cs236/bin/activate
python -m pip install -r requirements.txt
set -a
source .env
set +a
```

Wait for PostgreSQL to be ready, then populate:

```bash
sudo docker compose exec -T postgres pg_isready -U "$DB_USER" -d "$DB_NAME"
python populate_database.py
```

`populate_database.py` adds the PostgreSQL JDBC driver package specified by `SPARK_JDBC_PACKAGE` in `.env.example`.

After population, add indexes for the UI filters:

```bash
sudo docker compose exec -T postgres psql -U "$DB_USER" -d "$DB_NAME" < sql/phase3_indexes.sql
```

## 3. WebUI

In the same WSL terminal:

```bash
streamlit run app.py
```

Open the local URL printed by Streamlit, usually `http://localhost:8501`.

The UI can select the NYC, LA, or unified dataset. Can combine city, date, status, organization, request-type, channel, and postal-code filters and page through matching rows. PostgreSQL applies the filters and `LIMIT`/`OFFSET`, so the UI does not load the complete tables. The optional insert form writes a record directly into `merged_requests` with a generated primary-key ID.

## Helpful Notes

- Keep `.env` private.
- `sudo docker compose down` stops the service but keeps the data.
- `sudo docker compose down -v` deletes the database volume and all rows; DO NOT RUN!!!!
- Ensure you are running Ubuntu 24.04 using WSL before following this guide. 

