## sfme

sfme means service for me, it's a personal project to run some services for myself.

### Docker first login flow
1. Build image:
   ```bash
   docker compose build
   ```
2. Run one-time interactive login (enter phone/code/2FA when prompted):
   ```bash
   docker compose --profile login run --rm sfme-login
   ```
3. Start background service with auto restart:
   ```bash
   docker compose up -d sfme
   ```

### Testing auto restart

`restart: always` only triggers on process crash, not on `docker kill` / `docker stop` (those are treated as manual stops).

To simulate a crash and verify auto restart:

```bash
# Get the container's main process PID on the host
docker inspect --format '{{.State.Pid}}' sfme

# Kill it from the host (treated as crash, not manual stop)
kill -9 <PID>

# Verify restart
docker inspect -f 'started={{.State.StartedAt}} restart={{.RestartCount}} running={{.State.Running}}' sfme
```

A successful restart shows `RestartCount` incremented and `StartedAt` updated.
