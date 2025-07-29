use chrono;
use dashmap::DashMap;
use futures::future;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub name: String,
    pub email: String,
    pub age: u8,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateUserRequest {
    pub name: String,
    pub email: String,
    pub age: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateUserRequest {
    pub name: Option<String>,
    pub email: Option<String>,
    pub age: Option<u8>,
}

type Database = Arc<DashMap<String, User>>;

#[derive(Debug)]
pub enum DatabaseError {
    UserNotFound,
    UserAlreadyExists,
    ValidationError(String),
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DatabaseError::UserNotFound => write!(f, "User not found"),
            DatabaseError::UserAlreadyExists => write!(f, "User already exists"),
            DatabaseError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
        }
    }
}

impl std::error::Error for DatabaseError {}

#[derive(Debug, Default, Clone)]
pub struct ServiceStats {
    pub total_operations: u64,
    pub create_count: u64,
    pub read_count: u64,
    pub update_count: u64,
    pub delete_count: u64,
    pub parallel_operations: u64,
}

pub struct UserService {
    db: Database,
    stats: Arc<DashMap<(), ServiceStats>>,
}

impl UserService {
    pub fn new() -> Self {
        Self {
            db: Arc::new(DashMap::new()),
            stats: Arc::new(DashMap::new()),
        }
    }

    pub async fn create_user(&self, req: CreateUserRequest) -> Result<User, DatabaseError> {
        self.validate_user_data(&req).await?;
        let user = User {
            id: Uuid::new_v4().to_string(),
            name: req.name,
            email: req.email.to_lowercase(),
            age: req.age,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        if self.db.contains_key(&user.id) {
            return Err(DatabaseError::UserAlreadyExists);
        }

        self.db.insert(user.id.clone(), user.clone());
        self.increment_stat(|stats| stats.create_count += 1).await;
        Ok(user)
    }

    pub async fn get_user(&self, id: &str) -> Result<User, DatabaseError> {
        match self.db.get(id) {
            Some(user) => {
                self.increment_stat(|stats| stats.read_count += 1).await;
                Ok(user.value().clone())
            }
            None => Err(DatabaseError::UserNotFound),
        }
    }

    pub async fn update_user(
        &self,
        id: &str,
        req: UpdateUserRequest,
    ) -> Result<User, DatabaseError> {
        {
            let mut user = match self.db.get_mut(id) {
                Some(u) => u,
                None => return Err(DatabaseError::UserNotFound),
            };
            if let Some(name) = req.name {
                user.name = name;
            }
            if let Some(email) = req.email {
                user.email = email.to_lowercase();
            }
            if let Some(age) = req.age {
                user.age = age;
            }
            user.updated_at = chrono::Utc::now();
        }
        self.increment_stat(|stats| stats.update_count += 1).await;
        self.get_user(id).await
    }

    pub async fn delete_user(&self, id: &str) -> Result<User, DatabaseError> {
        match self.db.remove(id) {
            Some((_, user)) => {
                self.increment_stat(|stats| stats.delete_count += 1).await;
                Ok(user)
            }
            None => Err(DatabaseError::UserNotFound),
        }
    }

    pub async fn list_users(&self) -> Result<Vec<User>, DatabaseError> {
        let users = self.db.iter().map(|kv| kv.value().clone()).collect();
        self.increment_stat(|stats| stats.read_count += 1).await;
        Ok(users)
    }

    pub async fn bulk_create_users(
        self: Arc<Self>,
        requests: Vec<CreateUserRequest>,
    ) -> Vec<Result<User, DatabaseError>> {
        println!(
            "🎯 [Rayon] Transforming {} requests in parallel (to_uppercase)...",
            requests.len()
        );

        let processed: Vec<_> = requests
            .into_par_iter()
            .map(|req| CreateUserRequest {
                name: req.name.to_uppercase(),
                email: req.email,
                age: req.age,
            })
            .collect();

        println!("✅ [Rayon] Transformation done.");

        let mut results = Vec::with_capacity(processed.len());
        const BATCH_SIZE: usize = 5000;

        for (i, batch) in processed.chunks(BATCH_SIZE).enumerate() {
            println!(
                "🚀 [Tokio] Spawning async tasks for batch #{} ({} users)...",
                i + 1,
                batch.len()
            );

            let tasks = batch.iter().cloned().map(|req| {
                let svc = Arc::clone(&self);
                tokio::spawn(async move {
                    println!("⚙️ [Tokio] Creating user: {}", req.name);
                    svc.create_user(req).await
                })
            });

            let batch_results = futures::future::join_all(tasks).await;
            println!("✅ [Tokio] Batch #{} finished.", i + 1);

            results.extend(
                batch_results.into_iter().map(|r| {
                    r.unwrap_or_else(|e| Err(DatabaseError::ValidationError(e.to_string())))
                }),
            );
        }

        self.increment_stat(|stats| stats.parallel_operations += 1)
            .await;

        println!(
            "📊 [Stat] Total batches processed: {}",
            (processed.len() + BATCH_SIZE - 1) / BATCH_SIZE
        );
        results
    }

    pub async fn search_users_parallel(&self, query: &str) -> Result<Vec<User>, DatabaseError> {
        let users = self.list_users().await?;
        let query = query.to_lowercase();
        let results: Vec<User> = users
            .into_par_iter()
            .filter(|user| {
                user.name.to_lowercase().contains(&query)
                    || user.email.to_lowercase().contains(&query)
            })
            .collect();
        Ok(results)
    }

    pub async fn fast_concurrent_operations(self: Arc<Self>) -> Result<(), DatabaseError> {
        println!("🚀 Running 5 FAST concurrent operations...");
        let start = Instant::now();

        let reqs = (0..5)
            .map(|i| CreateUserRequest {
                name: format!("Fast User {}", i),
                email: format!("fast{}@demo.com", i),
                age: 20 + i as u8,
            })
            .collect::<Vec<_>>();

        let handles = reqs.into_iter().map(|req| {
            let service = Arc::clone(&self);
            async move { service.create_user(req).await }
        });

        let results = future::join_all(handles).await;

        let duration = start.elapsed();
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(user) => println!("✅ Fast Task {}: Created {}", i, user.name),
                Err(_) => println!("❌ Fast Task {}: Failed", i),
            }
        }
        println!("✅ Fast concurrent ops done in {:?}", duration);
        Ok(())
    }

    pub async fn bulk_insert_concurrent(
        self: Arc<Self>,
        count: usize,
    ) -> Result<(), DatabaseError> {
        println!("🚀 Bulk insert {} users with rayon + concurrent", count);
        let start = Instant::now();

        let requests: Vec<_> = (0..count)
            .map(|i| CreateUserRequest {
                name: format!("BulkConcurrent {}", i),
                email: format!("bulk{}@demo.com", i),
                age: 20 + (i % 80) as u8,
            })
            .collect();

        let processed: Vec<_> = requests
            .into_par_iter()
            .map(|req| {
                let name = req.name.to_uppercase();
                CreateUserRequest { name, ..req }
            })
            .collect();

        let handles = processed.into_iter().map(|req| {
            let service = Arc::clone(&self);
            async move { service.create_user(req).await }
        });

        let results = future::join_all(handles).await;
        let success = results.iter().filter(|r| r.is_ok()).count();
        let duration = start.elapsed();

        println!("✅ Inserted {} users in {:?}", success, duration);
        Ok(())
    }

    pub async fn bulk_save_to_csv(
        &self,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        let users = self.list_users().await?;
        let serialize_start = Instant::now();

        let mut wtr = csv::Writer::from_writer(vec![]);
        for user in &users {
            wtr.serialize(user)?;
        }
        let serialized_data = wtr.into_inner()?;
        let write_start = Instant::now();

        let mut file = File::create(path).await?;
        file.write_all(&serialized_data).await?;
        file.flush().await?;

        let duration = start.elapsed();
        let count = users.len();

        println!(
            "✅ Saved {} users to {} in {:?} (serialize: {:?}, write: {:?})",
            count,
            path,
            duration,
            serialize_start.elapsed(),
            write_start.elapsed()
        );
        Ok(())
    }

    pub async fn bulk_load_from_csv(
        self: Arc<Self>,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();

        let mut file = File::open(path).await?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).await?;
        let read_duration = start.elapsed();

        let cursor = std::io::Cursor::new(contents);
        let mut rdr = csv::Reader::from_reader(cursor);

        let parse_start = Instant::now();
        let mut requests = Vec::new();
        for result in rdr.deserialize() {
            let req: CreateUserRequest =
                result.map_err(|e| format!("CSV deserialize error: {}", e))?;
            requests.push(req);
        }
        let parse_duration = parse_start.elapsed();

        println!(
            "🚀 Loading {} users from {}... (read: {:?}, parse: {:?})",
            requests.len(),
            path,
            read_duration,
            parse_duration
        );

        let insert_start = Instant::now();
        let handles = requests.into_iter().map(|req| {
            let service = Arc::clone(&self);
            tokio::spawn(async move {
                let _ = service.create_user(req).await;
            })
        });

        let _ = futures::future::join_all(handles).await;
        let total_duration = start.elapsed();

        println!(
            "✅ Loaded {} users from {} in {:?} (insert: {:?})",
            self.db.len(),
            path,
            total_duration,
            insert_start.elapsed()
        );
        Ok(())
    }

    pub async fn complex_user_operation(&self, id: &str) -> Result<User, DatabaseError> {
        const MAX_RETRIES: u32 = 3;
        const RETRY_DELAY: Duration = Duration::from_millis(100);

        for attempt in 1..=MAX_RETRIES {
            match self.get_user(id).await {
                Ok(mut user) => {
                    sleep(Duration::from_millis(50)).await;
                    user.name = format!("Processed: {}", user.name);
                    let update_req = UpdateUserRequest {
                        name: Some(user.name.clone()),
                        email: None,
                        age: None,
                    };
                    return self.update_user(id, update_req).await;
                }
                Err(DatabaseError::UserNotFound) => return Err(DatabaseError::UserNotFound),
                Err(_) if attempt < MAX_RETRIES => {
                    println!("🔁 Retry attempt {} due to temporary issue", attempt);
                    sleep(RETRY_DELAY).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(DatabaseError::UserNotFound)
    }

    async fn validate_user_data(&self, req: &CreateUserRequest) -> Result<(), DatabaseError> {
        sleep(Duration::from_millis(10)).await;
        if req.name.is_empty() {
            return Err(DatabaseError::ValidationError(
                "Name cannot be empty".to_string(),
            ));
        }
        if !req.email.contains('@') {
            return Err(DatabaseError::ValidationError(
                "Invalid email format".to_string(),
            ));
        }
        if req.age < 13 || req.age > 120 {
            return Err(DatabaseError::ValidationError(
                "Age must be between 13 and 120".to_string(),
            ));
        }
        Ok(())
    }

    async fn increment_stat<F>(&self, updater: F)
    where
        F: FnOnce(&mut ServiceStats),
    {
        let mut stats = self.stats.entry(()).or_default();
        stats.total_operations += 1;
        updater(&mut stats);
    }

    pub async fn get_stats(&self) -> ServiceStats {
        self.stats
            .get(&())
            .map(|s| s.value().clone())
            .unwrap_or_default()
    }
}

pub async fn run_demo(service: Arc<UserService>) {
    println!("🚀 Starting Advanced Rust CRUD Demo with Parallel Processing\n");
    println!(
        "🔧 Tokio workers: {}",
        tokio::runtime::Handle::current().metrics().num_workers()
    );
    println!("🔧 Rayon threads: {}", rayon::current_num_threads());

    println!("=== 🔧 Basic CRUD ===");
    let start = Instant::now();
    let create_req = CreateUserRequest {
        name: "John Doe".to_string(),
        email: "john@example.com".to_string(),
        age: 30,
    };

    match service.create_user(create_req).await {
        Ok(user) => {
            println!("✅ Created: {} [Time: {:?}]", user.name, start.elapsed());
            let start = Instant::now();
            match service.get_user(&user.id).await {
                Ok(found) => println!("✅ Found: {} [Time: {:?}]", found.name, start.elapsed()),
                Err(e) => println!("❌ Get failed: {} [Time: {:?}]", e, start.elapsed()),
            }
            let start = Instant::now();
            let update = UpdateUserRequest {
                name: Some("John Smith".to_string()),
                email: None,
                age: Some(31),
            };
            match service.update_user(&user.id, update).await {
                Ok(updated) => {
                    println!("✅ Updated: {} [Time: {:?}]", updated.name, start.elapsed())
                }
                Err(e) => println!("❌ Update failed: {} [Time: {:?}]", e, start.elapsed()),
            }
        }
        Err(e) => println!("❌ Create failed: {} [Time: {:?}]", e, start.elapsed()),
    }

    println!(
        "\n=== 🔥 BULK: 10.000.000 USERS (Rayon(CPU Abound) + Tokio(IO Abound) + DashMap) ==="
    );
    let bulk_req: Vec<_> = (0..10_000_000)
        .map(|i| CreateUserRequest {
            name: format!("BulkUser{}", i),
            email: format!("user{}@bulk.com", i),
            age: 20 + (i % 80) as u8,
        })
        .collect();

    let start = Instant::now();
    let results = service.clone().bulk_create_users(bulk_req).await;
    let success = results.iter().filter(|r| r.is_ok()).count();
    println!(
        "✅ Bulk done in {:?} | {} success",
        start.elapsed(),
        success
    );

    println!("\n=== ⚡ FAST Concurrent Tasks (5) ===");
    let start = Instant::now();
    let _ = service.clone().fast_concurrent_operations().await;
    println!("✅ Fast concurrent ops done in {:?}", start.elapsed());

    println!("\n=== 🚀 BULK Concurrent Insert (5000) ===");
    let start = Instant::now();
    let _ = service.clone().bulk_insert_concurrent(5000).await;
    println!("✅ Bulk concurrent insert done in {:?}", start.elapsed());

    println!("\n=== 📊 Final Stats ===");
    let stats = service.get_stats().await;
    println!("Total ops: {}", stats.total_operations);
    println!(
        "Creates: {}, Reads: {}, Updates: {}, Deletes: {}",
        stats.create_count, stats.read_count, stats.update_count, stats.delete_count
    );
    println!("Parallel batches: {}", stats.parallel_operations);

    println!("\n=== 💾 SAVE TO CSV ===");
    let csv_path = "users_export.csv";
    if let Err(e) = service.bulk_save_to_csv(csv_path).await {
        eprintln!("❌ Save to CSV failed: {}", e);
    }

    println!("\n=== 📥 LOAD FROM CSV ===");
    if let Err(e) = service.clone().bulk_load_from_csv(csv_path).await {
        eprintln!("❌ Load from CSV failed: {}", e);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = Arc::new(UserService::new());
    run_demo(service).await;
    Ok(())
}
