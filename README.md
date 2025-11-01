# LightCMDB

A lightweight CMDB demo built with **Go + client-go + SQLite**, developed as part of a cloud-native learning plan (Week 3).

---

## 🚀 Features
- Watches **Pods** and **Nodes** in a Kubernetes/k3s cluster using client-go informers  
- Stores real-time resource data into **SQLite** (pure Go driver, no CGO needed)  
- Exposes REST APIs for querying resources  
- Supports namespace filtering  

---

## ⚙️ Tech Stack
- **Language:** Go 1.21+
- **Frameworks:** client-go v0.29, modernc.org/sqlite
- **Platform:** Kubernetes / k3s
- **Database:** SQLite
- **Environment:** Linux / Windows compatible

---

## 🧩 API Endpoints
| Method | Endpoint | Description |
|--------|-----------|-------------|
| GET | `/healthz` | Health check |
| GET | `/cmdb/pods` | List all Pods |
| GET | `/cmdb/pods?ns=default` | List Pods by namespace |
| GET | `/cmdb/nodes` | List all Nodes |

---

## 🧱 Quick Start
```bash
go mod tidy
go run main.go
