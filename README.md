# acm_board_backend

## Deploy

1. Copy this project to the server, for example `/opt/acm_board_backend2`.
2. Create a Python 3.11+ virtual environment and install the dependencies.
3. Copy `.env.example` to `.env` and fill in the real PTA and Kafka values.
4. Start the service:

```bash
uvicorn app:app --host 0.0.0.0 --port 8090
```

5. If you use `systemd`, a sample unit file is provided at `deploy/systemd/acm-board-backend.service`.
