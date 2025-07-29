# 🦀 Example Async Tokio + Rayon + Dashmap

Example Fully asynchronous and parallelized CRUD system using **Tokio**, **Rayon**, **DashMap**, and **Serde CSV** — example with 10 **million** users


```sh
✅ [Tokio] Batch #2000 finished.
📊 [Stat] Total batches processed: 2000
✅ Bulk done in 362.290654819s | 10000000 success

=== ⚡ FAST Concurrent Tasks (5) ===
🚀 Running 5 FAST concurrent operations...
✅ Fast Task 0: Created Fast User 0
✅ Fast Task 1: Created Fast User 1
✅ Fast Task 2: Created Fast User 2
✅ Fast Task 3: Created Fast User 3
✅ Fast Task 4: Created Fast User 4
✅ Fast concurrent ops done in 434.620426ms
✅ Fast concurrent ops done in 434.659968ms

=== 🚀 BULK Concurrent Insert (5000) ===
🚀 Bulk insert 5000 users with rayon + concurrent
✅ Inserted 5000 users in 94.353243ms
✅ Bulk concurrent insert done in 95.786411ms

=== 📊 Final Stats ===
Total ops: 10005010
Creates: 10005006, Reads: 2, Updates: 1, Deletes: 0
Parallel batches: 1

=== 💾 SAVE TO CSV ===
✅ Saved 10005006 users to users_export.csv in 79.240683972s (serialize: 71.582299106s, write: 1.330019724s)

=== 📥 LOAD FROM CSV ===
🚀 Loading 10005006 users from users_export.csv... (read: 2.184853569s, parse: 81.296040449s)

✅ Loaded 20010012 users from users_export.csv in 225.549932877s (insert: 142.068717244s)
```