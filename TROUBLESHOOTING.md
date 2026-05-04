# Troubleshooting Guide

## If Containers Still Disconnect

### 1. **Out of Disk Space**
```powershell
# Check disk usage
docker system df
docker system prune  # Clean up unused images/containers

# If still full, rebuild compose without debug volume
# Remove: `app_cache:/tmp` from compose.yaml
```

### 2. **MongoDB Connection Issues**
```powershell
# Check MongoDB health
docker logs mongo_db | tail -50

# Verify connection string
docker exec app_service printenv | grep MONGO

# Test connection manually
docker exec mongo_db mongosh --eval "db.adminCommand('ping')"
```

### 3. **Memory Exhaustion**
```powershell
# Monitor during run
docker stats --no-stream

# If memory keeps growing:
# - Reduce batch size in cleanLayer: 50000 -> 25000
# - Reduce chunk size in rawLayer: 50000 -> 25000
# - Reduce max pool size in project.py: 20 -> 10
```

### 4. **App Container Crashing**
```powershell
# Check logs
docker logs app_service --tail 100

# Look for:
# - MemoryError: Reduce batch sizes
# - ConnectionFailure: Check MongoDB logs
# - ServerSelectionTimeoutError: Increase timeouts further

# If timeout errors persist, in project.py increase:
serverSelectionTimeoutMS=60000  # from 30000
socketTimeoutMS=120000          # from 60000
```

### 5. **Slow Performance**
```powershell
# Enable timing in logs - add this to each layer:
import time
start = time.time()
# ... code ...
print(f"Layer took {time.time() - start:.2f} seconds")

# Compare before/after each fix
# Typical times with fixes:
# - rawLayer: 10-20s
# - cleanLayer: 30-60s (was 300-400s)
# - aggregateLayer: 5-10s
# - performanceLayer: 10-20s
```

---

## If Performance is Still Slow

### Check Resource Limits
Ensure Docker is using enough resources:
```powershell
# Windows: Docker Desktop → Settings → Resources
# Recommended:
# - CPUs: 4+
# - Memory: 8GB+
# - Disk: 30GB+
```

### Further Optimize Batch Sizes

Edit the batch sizes in layers:
```python
# rawLayer.py (line ~24)
chunksize = 25_000  # Smaller chunks = more iterations but less memory per chunk

# cleanLayer.py (line ~11)
def load_raw_data_batches(self, batch_size: int = 25000):  # Smaller
```

### Add Caching
Add intermediate result caching to avoid reprocessing:
```python
# In cleanLayer.py run() method, before cleaning:
if self.clean_collection.count_documents({}) > 0:
    print("Clean data already exists, skipping...")
    return
```

---

## MongoDB Tips

### Check if data persists
```powershell
# After first run
docker exec mongo_db mongosh --eval "db.raw.rawData.count()"

# Should show number of documents
# If always 0, data not persisting to volume
```

### View actual data
```powershell
docker exec mongo_db mongosh --eval "db.raw.rawData.findOne()"
docker exec mongo_db mongosh --eval "db.aggregated.summary.findOne()"
```

### Check indexes
```powershell
docker exec mongo_db mongosh --eval "db.raw.cleanData.getIndexes()"
```

---

## Windows-Specific Issues

### PowerShell Execution Policy
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
```

### File Path Issues
If you get path errors, ensure all paths use forward slashes in Docker:
```yaml
# Correct (use / not \)
volumes:
  - ../project:/app/project
  - ../src:/app/src
  
# Wrong - don't use backslashes in Docker
volumes:
  - ..\project:/app/project  # ❌
```

### Docker Desktop Not Running
```powershell
# Check if Docker is running
docker ps  # Should work

# If not, start Docker Desktop manually
# Or via PowerShell:
Start-Process 'C:\Program Files\Docker\Docker\Docker Desktop.exe'
```

---

## Log Analysis

### Look for these patterns in `docker logs app_service`:

**Good logs:**
```
BEGIN RAW LAYER
- CSV loaded: /app/taxi_trip_data.csv
- Inserted chunk 1: 50000 records
- Total inserted records: XXXXX
END RAW LAYER
BEGIN CLEAN LAYER
- Loading raw data from MongoDB in batches of 50000...
Batch 1: inserted 49000 cleaned records
END CLEAN LAYER
BEGIN PERFORMANCE TESTING LAYER
- Total documents in cleanData: XXXXX
- Query WITH Index: 0.0512 seconds
END PERFORMANCE TESTING LAYER
```

**Bad logs:**
```
MemoryError                          # Batch size too large
ConnectionFailure                    # MongoDB timeout
Killed                              # OOM kill
BrokenPipeError                     # Container network issue
docker_uri not defined              # Code bug (should be fixed)
```

---

## Recovery Steps

If the process fails mid-run:

```powershell
# 1. Stop all containers
docker-compose down

# 2. Remove MongoDB data (fresh start)
docker volume rm big-data-final-project_mongodb_data

# 3. Rebuild containers
docker-compose build --no-cache

# 4. Restart
docker-compose up

# OR keep data and restart
docker-compose up --build
```

---

## Performance Benchmarking

To measure improvements:

```powershell
# Before running, note:
$start = Get-Date

# Run process
docker-compose up

# After completion
$end = Get-Date
$duration = ($end - $start).TotalSeconds
Write-Host "Total time: $duration seconds"

# Compare to previous runs
# Should see 3-5x improvement with these fixes
```

---

## When to Increase Timeouts Further

If you see `ServerSelectionTimeoutError` despite the fixes:

In `project.py`, increase:
```python
client = MongoClient(
    mongodb_uri,
    serverSelectionTimeoutMS=60000,   # 60 seconds
    connectTimeoutMS=60000,
    socketTimeoutMS=120000,           # 2 minutes for long operations
    maxPoolSize=10,                   # Reduce if memory is tight
    minPoolSize=1,
)
```

---

## FAQ

**Q: How much disk space is needed?**
A: ~1-2x the size of your CSV file, plus MongoDB overhead. For a 1GB CSV, have 3GB free.

**Q: Can I process larger datasets?**
A: Yes! Just reduce batch sizes and the process will iterate more times but use less memory each time.

**Q: Why is Clean Layer still slow?**
A: Check if `.apply()` is still in the code. Some pandas versions may need the fix reapplied. Also verify batch size was changed.

**Q: Should I keep MongoDB running between runs?**
A: Yes! Restarting it each time adds 30-40s overhead. Just use `docker-compose down` → `docker-compose up` (not rebuild).

**Q: How do I know if the fix worked?**
A: 
- No container disconnects
- Completes in <3 minutes
- `docker stats` shows stable memory (not climbing)
- All 4 layers complete without errors
