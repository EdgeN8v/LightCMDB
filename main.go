package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "errors"
    "fmt"
    "log"
    "net/http"
    "path/filepath"
    "strings"
    "time"

    _ "modernc.org/sqlite"

    corev1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/fields"
    "k8s.io/client-go/informers"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/tools/cache"
    "k8s.io/client-go/tools/clientcmd"
)

const (
    dsn = "file:cmdb.db?cache=shared&mode=rwc"
)

// ---------- DB ----------

func openDB() (*sql.DB, error) {
    db, err := sql.Open("sqlite", dsn)
    if err != nil {
        return nil, err
    }
    db.SetMaxOpenConns(1) // SQLite 单连接足够
    return db, nil
}

func initSchema(db *sql.DB) error {
    podTable := `
CREATE TABLE IF NOT EXISTS pods(
    uid TEXT PRIMARY KEY,
    name TEXT,
    namespace TEXT,
    phase TEXT,
    node_name TEXT,
    pod_ip TEXT,
    created_at TEXT,
    updated_at TEXT
);`
    nodeTable := `
CREATE TABLE IF NOT EXISTS nodes(
    name TEXT PRIMARY KEY,
    labels TEXT,
    capacity_cpu TEXT,
    capacity_mem TEXT,
    internal_ip TEXT,
    created_at TEXT,
    updated_at TEXT
);`
    _, err := db.Exec(podTable)
    if err != nil {
        return err
    }
    _, err = db.Exec(nodeTable)
    return err
}

func upsertPod(db *sql.DB, p *corev1.Pod) error {
    if p == nil {
        return errors.New("nil pod")
    }
    uid := string(p.UID)
    now := time.Now().Format(time.RFC3339)
    _, err := db.Exec(`
INSERT INTO pods(uid,name,namespace,phase,node_name,pod_ip,created_at,updated_at)
VALUES(?,?,?,?,?,?,?,?)
ON CONFLICT(uid) DO UPDATE SET
 name=excluded.name,
 namespace=excluded.namespace,
 phase=excluded.phase,
 node_name=excluded.node_name,
 pod_ip=excluded.pod_ip,
 updated_at=excluded.updated_at
`, uid, p.Name, p.Namespace, string(p.Status.Phase), p.Spec.NodeName, p.Status.PodIP, now, now)
    return err
}

func deletePod(db *sql.DB, uid string) error {
    _, err := db.Exec(`DELETE FROM pods WHERE uid=?`, uid)
    return err
}

func upsertNode(db *sql.DB, n *corev1.Node) error {
    if n == nil {
        return errors.New("nil node")
    }
    // 简化：取 CPU/内存为字符串、InternalIP
    cpu := n.Status.Capacity.Cpu().String()
    mem := n.Status.Capacity.Memory().String()
    ip := ""
    for _, a := range n.Status.Addresses {
        if a.Type == corev1.NodeInternalIP {
            ip = a.Address
            break
        }
    }
    // 展平 labels
    var labels []string
    for k, v := range n.Labels {
        labels = append(labels, fmt.Sprintf("%s=%s", k, v))
    }
    now := time.Now().Format(time.RFC3339)
    _, err := db.Exec(`
INSERT INTO nodes(name,labels,capacity_cpu,capacity_mem,internal_ip,created_at,updated_at)
VALUES(?,?,?,?,?,?,?)
ON CONFLICT(name) DO UPDATE SET
 labels=excluded.labels,
 capacity_cpu=excluded.capacity_cpu,
 capacity_mem=excluded.capacity_mem,
 internal_ip=excluded.internal_ip,
 updated_at=excluded.updated_at
`, n.Name, strings.Join(labels, ","), cpu, mem, ip, now, now)
    return err
}

func deleteNode(db *sql.DB, name string) error {
    _, err := db.Exec(`DELETE FROM nodes WHERE name=?`, name)
    return err
}

// ---------- K8s ----------

func getClientset() (*kubernetes.Clientset, error) {
    kubeconfig := filepath.Join("/etc/rancher/k3s/k3s.yaml")
    cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
    if err != nil {
        return nil, err
    }
    return kubernetes.NewForConfig(cfg)
}

// ---------- HTTP DTO ----------

type PodRow struct {
    UID       string `json:"uid"`
    Name      string `json:"name"`
    Namespace string `json:"namespace"`
    Phase     string `json:"phase"`
    NodeName  string `json:"nodeName"`
    PodIP     string `json:"podIP"`
    UpdatedAt string `json:"updatedAt"`
}

type NodeRow struct {
    Name       string `json:"name"`
    Labels     string `json:"labels"`
    CPU        string `json:"cpu"`
    Memory     string `json:"memory"`
    InternalIP string `json:"internalIP"`
    UpdatedAt  string `json:"updatedAt"`
}

// ---------- HTTP Handlers ----------

func podsAPI(db *sql.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        ns := r.URL.Query().Get("ns")
        var rows *sql.Rows
        var err error
        if ns == "" {
            rows, err = db.Query(`SELECT uid,name,namespace,phase,node_name,pod_ip,updated_at FROM pods ORDER BY namespace,name`)
        } else {
            rows, err = db.Query(`SELECT uid,name,namespace,phase,node_name,pod_ip,updated_at FROM pods WHERE namespace=? ORDER BY name`, ns)
        }
        if err != nil {
            http.Error(w, err.Error(), 500)
            return
        }
        defer rows.Close()
        var out []PodRow
        for rows.Next() {
            var p PodRow
            if err := rows.Scan(&p.UID, &p.Name, &p.Namespace, &p.Phase, &p.NodeName, &p.PodIP, &p.UpdatedAt); err != nil {
                http.Error(w, err.Error(), 500)
                return
            }
            out = append(out, p)
        }
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(out)
    }
}

func nodesAPI(db *sql.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        rows, err := db.Query(`SELECT name,labels,capacity_cpu,capacity_mem,internal_ip,updated_at FROM nodes ORDER BY name`)
        if err != nil {
            http.Error(w, err.Error(), 500)
            return
        }
        defer rows.Close()
        var out []NodeRow
        for rows.Next() {
            var n NodeRow
            if err := rows.Scan(&n.Name, &n.Labels, &n.CPU, &n.Memory, &n.InternalIP, &n.UpdatedAt); err != nil {
                http.Error(w, err.Error(), 500)
                return
            }
            out = append(out, n)
        }
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(out)
    }
}

// ---------- Bootstrap ----------

func main() {
    log.SetFlags(log.LstdFlags | log.Lmicroseconds)

    // DB
    db, err := openDB()
    if err != nil {
        log.Fatalf("open db: %v", err)
    }
    if err := initSchema(db); err != nil {
        log.Fatalf("init schema: %v", err)
    }

    // K8s
    client, err := getClientset()
    if err != nil {
        log.Fatalf("load kubeconfig: %v", err)
    }

    // Informers（全命名空间）
    // 也可换成 factory := informers.NewSharedInformerFactoryWithOptions(client, 0, informers.WithNamespace("default"))
    factory := informers.NewSharedInformerFactory(client, 0)

    // Pod Informer
    podInformer := factory.Core().V1().Pods().Informer()
    podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            pod := obj.(*corev1.Pod)
            if err := upsertPod(db, pod); err != nil {
                log.Printf("[pods/add] %s/%s err=%v", pod.Namespace, pod.Name, err)
            } else {
                log.Printf("[pods/add] %s/%s", pod.Namespace, pod.Name)
            }
        },
        UpdateFunc: func(oldObj, newObj interface{}) {
            pod := newObj.(*corev1.Pod)
            if err := upsertPod(db, pod); err != nil {
                log.Printf("[pods/update] %s/%s err=%v", pod.Namespace, pod.Name, err)
            }
        },
        DeleteFunc: func(obj interface{}) {
            // Delete 时 obj 可能是 DeletedFinalStateUnknown
            switch t := obj.(type) {
            case *corev1.Pod:
                _ = deletePod(db, string(t.UID))
                log.Printf("[pods/del] %s/%s", t.Namespace, t.Name)
            case cache.DeletedFinalStateUnknown:
                if p, ok := t.Obj.(*corev1.Pod); ok {
                    _ = deletePod(db, string(p.UID))
                    log.Printf("[pods/delDFSU] %s/%s", p.Namespace, p.Name)
                }
            }
        },
    })

    // Node Informer（示例加了一个 field selector 的写法）
    nodeInformer := factory.Core().V1().Nodes().Informer()
    nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            n := obj.(*corev1.Node)
            if err := upsertNode(db, n); err != nil {
                log.Printf("[nodes/add] %s err=%v", n.Name, err)
            } else {
                log.Printf("[nodes/add] %s", n.Name)
            }
        },
        UpdateFunc: func(oldObj, newObj interface{}) {
            n := newObj.(*corev1.Node)
            if err := upsertNode(db, n); err != nil {
                log.Printf("[nodes/update] %s err=%v", n.Name, err)
            }
        },
        DeleteFunc: func(obj interface{}) {
            switch t := obj.(type) {
            case *corev1.Node:
                _ = deleteNode(db, t.Name)
                log.Printf("[nodes/del] %s", t.Name)
            case cache.DeletedFinalStateUnknown:
                if n, ok := t.Obj.(*corev1.Node); ok {
                    _ = deleteNode(db, n.Name)
                    log.Printf("[nodes/delDFSU] %s", n.Name)
                }
            }
        },
    })

    // 启动 informer
    stop := make(chan struct{})
    factory.Start(stop)
    // 等待缓存同步
    factory.WaitForCacheSync(stop)

    // HTTP
    mux := http.NewServeMux()
    mux.HandleFunc("/cmdb/pods", podsAPI(db))
    mux.HandleFunc("/cmdb/nodes", nodesAPI(db))
    mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })

    srv := &http.Server{
        Addr:              ":8080",
        Handler:           mux,
        ReadHeaderTimeout: 5 * time.Second,
    }

    log.Println("LightCMDB Week3 started on :8080")
    log.Fatal(srv.ListenAndServe())

    // 优雅退出（保留示例）
    _ = fields.Everything // 引用避免未使用（示例中没有真正用到）
    _ = metav1.NamespaceAll
    _ = context.Background()
}

