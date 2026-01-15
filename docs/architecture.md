# EXO 分散式 AI 推理框架 - 完整技術架構分析

## 1. 專案概述

### 1.1 核心目標

EXO 是一個分散式 AI 推理框架，旨在：

- 在消費級設備上構建分散式 AI 推理叢集（連接筆記型電腦、Mac Studio 等）
- 實現自動設備發現和拓撲感知的模型分片
- 支援 RDMA over Thunderbolt 5 的高速分散式推理
- 提供零配置的即插即用叢集體驗

### 1.2 技術堆疊

| 類別 | 技術選型 |
|------|----------|
| 核心語言 | Python 3.13+（Pydantic 2.11+、anyio 4.11） |
| 網路層 | Rust + libp2p（PyO3 綁定） |
| 推理引擎 | MLX（macOS GPU）、MLX[cpu]（Linux CPU） |
| API 框架 | FastAPI + Hypercorn |
| 序列化 | Pydantic JSON + msgpack |
| 拓撲圖 | rustworkx |
| 相容性 | OpenAI API（openai-harmony） |

### 1.3 支援平台

| 平台 | GPU 支援 | 狀態 |
|------|----------|------|
| macOS (Apple Silicon M3/M4) | MLX GPU 加速 | 完整支援 |
| Linux x86_64 | CPU 推理 | 支援中 |
| Linux (CUDA) | NVIDIA GPU | 開發中 |

---

## 2. 系統架構

### 2.1 五大核心系統

EXO 採用 Event Sourcing + CQRS 架構，包含五個主要系統：

```
┌─────────────────────────────────────────────────────────────────┐
│                         EXO 節點                                │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌────────┐│
│  │ Master  │  │ Worker  │  │ Runner  │  │   API   │  │Election││
│  │         │  │         │  │(進程)   │  │         │  │        ││
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  └───┬────┘│
│       │            │            │            │           │      │
│  ┌────┴────────────┴────────────┴────────────┴───────────┴────┐ │
│  │                    Topic Router (libp2p)                    │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

#### 2.1.1 Master 系統

**位置**: `src/exo/master/`

**職責**:
- 接收並處理來自 Worker 的命令（Command）
- 執行模型放置（Placement）演算法
- 生成全域事件並廣播到叢集
- 管理任務生命週期
- 維護全域事件序列的一致性

**關鍵檔案**:
| 檔案 | 功能 |
|------|------|
| `master/main.py` | Master 核心邏輯、命令處理、規劃迴圈 |
| `master/api.py` | FastAPI Web 服務（埠號 52415） |
| `master/placement.py` | 實例放置演算法 |
| `master/placement_utils.py` | 拓撲感知的分片策略 |

#### 2.1.2 Worker 系統

**位置**: `src/exo/worker/`

**職責**:
- 監聽全域事件並套用到本地狀態
- 執行計畫（Plan）以決定下一步操作
- 管理模型下載和 Runner 生命週期
- 報告資源利用情況（CPU、記憶體、網路）
- 與其他 Worker 進行 P2P 通訊

**關鍵檔案**:
| 檔案 | 功能 |
|------|------|
| `worker/main.py` | Worker 核心邏輯、事件處理、下載管理 |
| `worker/plan.py` | 狀態機邏輯（決定何時建立/銷毀 Runner） |
| `worker/runner/` | Runner 進程管理 |
| `worker/download/` | 模型分片下載系統 |
| `worker/engines/mlx/` | MLX 推理引擎整合 |

#### 2.1.3 Runner 系統

**位置**: `src/exo/worker/runner/`

**職責**:
- 在隔離進程中執行推理任務
- 管理單個模型分片的載入和執行
- 處理與其他 Runner 的分散式通訊（JACCL/Ring）
- 生成推理輸出 token

**狀態機流程**:
```
Idle → Connecting → Connected → Loading → Loaded → WarmingUp → Ready → Running
                                                                    ↓
                                                              Shutdown/Failed
```

**關鍵檔案**:
| 檔案 | 功能 |
|------|------|
| `runner/runner.py` | 主推理迴圈、狀態機實現 |
| `runner/runner_supervisor.py` | 進程生命週期管理 |
| `runner/bootstrap.py` | Runner 進程入口點 |

#### 2.1.4 API 系統

**位置**: `src/exo/master/api.py`

**端點列表**:
| 端點 | 方法 | 功能 |
|------|------|------|
| `/models` | GET | 列出可用模型 |
| `/instance/previews` | GET | 預覽可能的放置方案 |
| `/instance` | POST | 建立模型實例 |
| `/instance/{id}` | DELETE | 刪除實例 |
| `/v1/chat/completions` | POST | OpenAI 相容的聊天接口 |
| `/state` | GET | 獲取叢集全域狀態 |
| `/` | GET | Web 儀表板 |

#### 2.1.5 Election 系統

**位置**: `src/exo/shared/election.py`

**演算法**: 基於 Bully 演算法的變種，考慮：
- 時鐘同步（clock）
- 資歷（seniority）
- 命令計數（command count）

**特點**:
- 不穩定網路環境下自動恢復
- 支援候選/非候選節點模式
- 選舉期間暫停 API 服務

---

## 3. 通訊與路由層

### 3.1 Topic Router 架構

**位置**: `src/exo/routing/`

EXO 使用基於 libp2p 的 pub/sub 模式進行節點間通訊：

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Topic Router                                │
├─────────────────────────────────────────────────────────────────────┤
│  GLOBAL_EVENTS    ←─── Master 廣播已排序的全域事件                    │
│  LOCAL_EVENTS     ←─── Workers 發送本地事件（未排序）                  │
│  COMMANDS         ←─── API/Workers 發送操作命令到 Master              │
│  ELECTION_MSGS    ←─── 節點間選舉訊息                                 │
│  CONNECTION_MSGS  ←─── 網路拓撲發現（本地使用）                        │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.2 主題發布策略

| 主題 | 發布策略 | 說明 |
|------|----------|------|
| `GLOBAL_EVENTS` | Always | 始終廣播到所有節點 |
| `LOCAL_EVENTS` | Always | 始終發送到 Forwarder |
| `COMMANDS` | Always | 始終發送到 Master |
| `ELECTION_MESSAGES` | Always | 始終廣播參與選舉 |
| `CONNECTION_MESSAGES` | Never | 僅本地使用，不網路傳輸 |

### 3.3 核心類型

**位置**: `src/exo/routing/topics.py`

```python
TypedTopic[T]      # 類型化主題，支援 JSON 序列化/反序列化
PublishPolicy      # Never / Minimal / Always
TopicRouter[T]     # 管理訂閱者和網路發布
OrderedBuffer[T]   # 事件排序緩衝區
```

---

## 4. Event Sourcing 模式

### 4.1 核心概念

```
                    ┌─────────────┐
                    │   Command   │ (命令式：PlaceInstance, ChatCompletion)
                    └──────┬──────┘
                           │
                           ▼
┌──────────┐      ┌─────────────┐      ┌─────────────┐
│  State   │ ───▶ │   Master    │ ───▶ │   Event     │ (過去式：InstanceCreated)
└──────────┘      └─────────────┘      └──────┬──────┘
     ▲                                        │
     │                                        │
     └─────────── apply(state, event) ────────┘
```

### 4.2 事件類型

**位置**: `src/exo/shared/types/events.py`

| 類別 | 事件 | 說明 |
|------|------|------|
| 任務 | `TaskCreated` | 新任務建立 |
| | `TaskStatusUpdated` | 任務狀態更新 |
| | `TaskFailed` | 任務失敗 |
| | `TaskDeleted` | 任務刪除 |
| 實例 | `InstanceCreated` | 模型實例建立 |
| | `InstanceDeleted` | 實例刪除 |
| Runner | `RunnerStatusUpdated` | Runner 狀態變更 |
| | `RunnerDeleted` | Runner 移除 |
| 節點 | `NodePerformanceMeasured` | 效能測量結果 |
| | `NodeMemoryMeasured` | 記憶體測量結果 |
| | `NodeDownloadProgress` | 下載進度 |
| 拓撲 | `TopologyEdgeCreated` | 新連接建立 |
| | `TopologyEdgeDeleted` | 連接斷開 |

### 4.3 命令類型

**位置**: `src/exo/shared/types/commands.py`

| 命令 | 說明 |
|------|------|
| `PlaceInstance` | 請求放置模型實例 |
| `ChatCompletion` | 發起聊天推理請求 |
| `DeleteInstance` | 刪除模型實例 |
| `RequestCatchup` | 請求事件追趕 |

### 4.4 任務類型

**位置**: `src/exo/shared/types/tasks.py`

| 任務 | 說明 |
|------|------|
| `CreateRunner` | 建立新的 Runner 進程 |
| `DownloadModel` | 下載模型分片 |
| `ConnectToGroup` | 連接到分散式群組 |
| `LoadModel` | 載入模型到記憶體 |
| `StartWarmup` | 執行預熱推理 |
| `ChatCompletion` | 執行聊天推理 |
| `Shutdown` | 關閉 Runner |

### 4.5 狀態結構

**位置**: `src/exo/shared/types/state.py`

```python
State
├── instances: Dict[InstanceId, Instance]
│   ├── MlxRingInstance
│   └── MlxJacclInstance
│       ├── shard_assignments
│       │   ├── runner_to_shard: Dict[RunnerId, ShardMetadata]
│       │   └── node_to_runner: Dict[NodeId, RunnerId]
│       └── jaccl_coordinators: Dict[NodeId, str]
├── runners: Dict[RunnerId, RunnerStatus]
├── downloads: Dict[NodeId, List[DownloadProgress]]
├── tasks: Dict[TaskId, Task]
├── node_profiles: Dict[NodeId, NodePerformanceProfile]
├── last_seen: Dict[NodeId, datetime]
└── topology: Topology
    ├── nodes: Dict[NodeId, NodeInfo]
    └── connections: List[Connection]
```

---

## 5. 模型分片與分散式推理

### 5.1 分片策略

EXO 支援兩種並行化策略：

#### 5.1.1 Pipeline 並行

**位置**: `src/exo/shared/types/worker/shards.py`

```
Device 0          Device 1          Device 2
┌─────────┐      ┌─────────┐      ┌─────────┐
│Layer 0-20│ ──▶ │Layer 20-40│ ──▶ │Layer 40-61│
└─────────┘      └─────────┘      └─────────┘

特點：
- 按層分配到不同設備
- start_layer, end_layer（半開區間）
- 適用於任何模型
```

#### 5.1.2 Tensor 並行

```
                    Device 0              Device 1
Layer N ──▶  ┌──────────────────┐  ┌──────────────────┐
             │ hidden[:, 0:H/2] │  │ hidden[:, H/2:H] │
             └────────┬─────────┘  └────────┬─────────┘
                      │                     │
                      └──────── AllSum ─────┘
                                  │
                                  ▼
                            Output

約束：
- model.hidden_size % num_devices == 0
- 需要模型支援 supports_tensor=True
```

### 5.2 放置演算法

**位置**: `src/exo/master/placement.py`

```python
place_instance(command, topology, current_instances)
│
├── Step 1: 枚舉候選環（cycles）
│   └── topology.get_cycles() → 找出所有可用環路
│
├── Step 2: 記憶體過濾
│   └── filter_cycles_by_memory() → 確保總記憶體足夠
│
├── Step 3: 分片約束驗證
│   ├── Tensor: hidden_size % len(cycle) == 0
│   └── Pipeline: 預設支援
│
├── Step 4: 優先級排序
│   ├── 1. 最小 Thunderbolt 環（最優）
│   ├── 2. 最小非 Thunderbolt 環
│   └── 3. 單節點（最後選項）
│
└── Step 5: 生成分片分配
    └── get_shard_assignments_for_pipeline_parallel()
        ├── 按可用記憶體比例分配層
        └── 為每個節點建立 RunnerId
```

### 5.3 分散式通訊後端

#### 5.3.1 MlxRing

- 基於 MLX 原生 `distributed.Group`
- 使用 TCP Ring 拓撲
- 適合一般網路連接

#### 5.3.2 MlxJaccl

- JACCL = Joint All-Reduce Communication Library
- 支援 RDMA over Thunderbolt 5（macOS 26.2+）
- 延遲 < 1μs
- 座標器 IP 選擇優先級：EN0 > EN1 > non-TB5 > other

### 5.4 層包裝實現

**位置**: `src/exo/worker/engines/mlx/auto_parallel.py`

```python
# Pipeline 第一層
class PipelineFirstLayer:
    def __call__(self, x, ...):
        if rank != 0:
            x = mx.distributed.recv_like(x, rank-1, group)
        return self.layer(x)

# Pipeline 中間層
class PipelineMiddleLayer:
    def __call__(self, x, ...):
        result = self.layer(x)
        mx.distributed.send(result, rank+1, group)
        return mx.distributed.recv_like(..., rank-1, group)

# Pipeline 最後層
class PipelineLastLayer:
    def __call__(self, x, ...):
        x = mx.distributed.recv_like(x, rank-1, group)
        return self.layer(x)
```

---

## 6. MLX 推理引擎

### 6.1 引擎架構

**位置**: `src/exo/worker/engines/mlx/`

```
mlx/
├── generator/
│   ├── generate.py      # 主生成迴圈
│   ├── sampler.py       # 取樣策略
│   └── cache.py         # KV Cache 管理
├── auto_parallel.py     # 並行化層包裝
├── loader.py            # 模型載入器
└── utils.py             # 工具函數
```

### 6.2 支援的模型架構

| 模型系列 | 範例 | 特殊支援 |
|----------|------|----------|
| Llama | Llama 3.2, Llama 2 | 標準支援 |
| Qwen | Qwen 3 MoE | MoE 層支援 |
| DeepSeek | V3, V3.1 | Tensor 並行 |
| Kimi | K2 | 原生 4-bit 支援 |

### 6.3 KV Cache 優化

```python
# KV Cache 配置
KV_CACHE_BITS = None  # None=禁用量化，4/8=量化位數

# 快取類型
KVPrefixCache    # 前綴重用快取
QuantizedKVCache # 量化 KV 快取
```

### 6.4 取樣策略

**位置**: `src/exo/worker/engines/mlx/generator/sampler.py`

支援參數：
- `temperature`: 溫度控制
- `top_p`: 核心取樣
- `top_k`: Top-K 取樣
- `min_p`: 最小概率閾值

---

## 7. 網路層與節點發現

### 7.1 Rust Networking

**位置**: `rust/exo_pyo3_bindings/src/networking.rs`

基於 libp2p 的實現：
```
libp2p 0.56
├── libp2p-tcp      # TCP 傳輸
├── libp2p-mdns     # mDNS 本地發現
├── libp2p-identify # 節點身份交換
└── libp2p-ping     # 連接探測
```

### 7.2 自動發現流程

```
啟動 EXO 節點
│
├── Keypair 生成/載入
│   └── ~/.config/exo/node_id.keypair（持久化）
│
├── NetworkingHandle 初始化
│   ├── mDNS: 廣播 _exo._tcp
│   └── TCP: 52413（預設埠）
│
├── 發現其他節點
│   └── ConnectionMessage
│       ├── node_id（base58）
│       ├── connection_type
│       └── remote_ipv4, remote_tcp_port
│
└── 構建拓撲
    └── Topology.add_connection()
```

### 7.3 拓撲管理

**位置**: `src/exo/shared/topology.py`

```python
Topology
├── _graph: rustworkx.PyDiGraph[NodeInfo, Connection]
├── _node_id_to_rx_id_map: Dict[NodeId, int]
├── _edge_id_to_rx_id_map: Dict[Connection, int]
│
├── 方法：
│   ├── get_cycles()              # 取得所有環路
│   ├── get_subgraph_from_nodes() # 取得子圖
│   ├── is_thunderbolt_cycle()    # 檢查 Thunderbolt 環
│   ├── neighbours()              # 鄰居節點
│   └── contains_connection()     # 檢查連接存在
```

### 7.4 Thunderbolt 檢測

```python
def is_thunderbolt(connection: Connection) -> bool:
    return connection.ipv4_address.startswith("169.254")
```

---

## 8. API 設計

### 8.1 RESTful 端點

#### 模型列表

```http
GET /models

Response:
{
  "object": "list",
  "data": [
    {
      "id": "mlx-community/Llama-3.2-1B-Instruct-4bit",
      "object": "model",
      "created": 1234567890,
      "owned_by": "exo",
      "context_length": 8192,
      "supports_tensor": false
    }
  ]
}
```

#### 實例預覽

```http
GET /instance/previews?model_id=llama-3.2-1b

Response:
{
  "previews": [
    {
      "model_id": "mlx-community/Llama-3.2-1B-Instruct-4bit",
      "sharding": "Pipeline",
      "instance_meta": "MlxRing",
      "instance": {...},
      "memory_delta_by_node": {"node_id_1": 729808896},
      "error": null
    }
  ]
}
```

#### 建立實例

```http
POST /instance
Content-Type: application/json

{
  "instance": {...}
}

Response:
{
  "message": "Command received",
  "command_id": "cmd-xxx"
}
```

#### 聊天補全（OpenAI 相容）

```http
POST /v1/chat/completions
Content-Type: application/json

{
  "model": "mlx-community/Llama-3.2-1B-Instruct-4bit",
  "messages": [
    {"role": "system", "content": "You are helpful"},
    {"role": "user", "content": "Hello"}
  ],
  "stream": true,
  "temperature": 0.7,
  "max_tokens": 256
}

Response (SSE):
data: {"id":"cmd-xxx","choices":[{"delta":{"content":"Hello"},...}]}
data: {"id":"cmd-xxx","choices":[{"delta":{"content":" there"},...}]}
data: [DONE]
```

### 8.2 OpenAI 相容性

| 功能 | 支援狀態 |
|------|----------|
| Chat Completions | ✅ 完整支援 |
| Streaming | ✅ 完整支援 |
| Message Format | ✅ 支援 role, content, thinking |
| Token Count | ✅ openai-harmony |
| Function Calling | ❌ 未實現 |
| Vision | ❌ 未實現 |
| Embeddings | ❌ 未實現 |

---

## 9. 配置系統

### 9.1 目錄結構

**位置**: `src/exo/shared/constants.py`

```bash
# XDG 基礎目錄
EXO_CONFIG_HOME = ~/.config/exo      # 配置檔案
EXO_DATA_HOME   = ~/.local/share/exo # 資料檔案
EXO_CACHE_HOME  = ~/.cache/exo       # 快取檔案

# 關鍵路徑
EXO_NODE_ID_KEYPAIR = $EXO_CONFIG_HOME/node_id.keypair
EXO_MODELS_DIR      = $EXO_DATA_HOME/models
EXO_LOG             = $EXO_CACHE_HOME/exo.log
```

### 9.2 環境變數

| 變數 | 說明 |
|------|------|
| `EXO_HOME` | 覆蓋所有 XDG 目錄 |
| `EXO_MODELS_DIR` | 自訂模型目錄 |
| `XDG_CONFIG_HOME` | XDG 配置目錄 |
| `XDG_DATA_HOME` | XDG 資料目錄 |
| `XDG_CACHE_HOME` | XDG 快取目錄 |
| `EXO_TESTS` | 設為 1 啟用測試模式 |

### 9.3 命令列參數

```python
@dataclass
class Args:
    spawn_api: bool = True      # 啟動 HTTP API
    api_port: int = 52415       # API 埠號
    force_master: bool = False  # 強制為 Master
```

### 9.4 模型卡片

**位置**: `src/exo/shared/models/model_cards.py`

```python
MODEL_CARDS = {
    "deepseek-v3.1-8bit": ModelCard(
        model_id="mlx-community/DeepSeek-V3.1-8bit",
        metadata=ModelMetadata(
            storage_size=Memory.from_gb(713),
            n_layers=61,
            hidden_size=7168,
            supports_tensor=True
        )
    ),
    # ...更多模型
}
```

---

## 10. 測試架構

### 10.1 測試目錄結構

```
src/exo/
├── shared/tests/
│   ├── test_state_serialization.py
│   ├── test_election.py
│   ├── test_node_id_persistence.py
│   └── test_apply/
│       └── test_apply_node_download.py
├── master/tests/
│   ├── test_placement.py
│   ├── test_placement_utils.py
│   ├── test_topology.py
│   └── test_master.py
├── worker/tests/
│   ├── unittests/
│   │   ├── test_plan/
│   │   │   ├── test_download_and_loading.py
│   │   │   ├── test_warmup.py
│   │   │   ├── test_runner_lifecycle.py
│   │   │   └── test_task_forwarding.py
│   │   └── test_runner/
│   │       ├── test_event_ordering.py
│   │       └── test_runner_supervisor.py
│   └── test_mlx/
└── routing/tests/
    └── test_event_buffer.py
```

### 10.2 測試配置

```toml
# pyproject.toml
[tool.pytest.ini_options]
pythonpath = "."
asyncio_mode = "auto"
markers = ["slow: marks tests as slow"]
env = ["EXO_TESTS=1"]
addopts = "-m 'not slow'"
```

### 10.3 測試類型

| 類型 | 說明 | 範例 |
|------|------|------|
| 單元測試 | 測試單一函數/類 | `test_apply.py` |
| 整合測試 | 測試多模組互動 | `test_master.py` |
| 慢速測試 | 完整推理流程 | 標記 `@pytest.mark.slow` |

---

## 11. 完整資料流

### 11.1 推理請求流程

```
┌─────────┐
│ Client  │
└────┬────┘
     │ POST /v1/chat/completions
     ▼
┌─────────┐
│   API   │ ──────▶ Command.ChatCompletion
└────┬────┘
     │
     ▼
┌─────────┐
│ Master  │ ──────▶ Task.ChatCompletion
└────┬────┘         Event.TaskCreated
     │
     ▼ (GlobalEvent)
┌─────────┐
│ Worker  │ ──────▶ apply(state, event)
└────┬────┘         plan() → 執行任務
     │
     ▼
┌─────────┐
│ Runner  │ ──────▶ mlx_generate()
└────┬────┘         Event.ChunkGenerated × N
     │
     ▼ (GlobalEvent)
┌─────────┐
│   API   │ ──────▶ StreamingResponse (SSE)
└────┬────┘
     │
     ▼
┌─────────┐
│ Client  │ ←────── {"choices": [{"delta": {"content": "..."}}]}
└─────────┘
```

### 11.2 模型載入流程

```
Instance Created
     │
     ▼
┌─────────────────────────────────────┐
│ Worker.plan() → CreateRunner Task   │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│ RunnerSupervisor.run()              │
│ Runner: Idle → Connecting           │
│ → initialize_mlx()                  │
│ → 建立 MLX distributed.Group        │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│ Runner: Connected → Loading         │
│ → load_mlx_items()                  │
│ → 從磁碟載入權重（分片）              │
│ → 套用並行化層包裝                   │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│ Runner: Loaded → WarmingUp          │
│ → warmup_inference()                │
│ → 生成 50 個預熱 token               │
│ → mx.distributed.all_sum (barrier)  │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│ Runner: Ready → Running             │
│ → 準備接受推理請求                   │
└─────────────────────────────────────┘
```

---

## 12. 部署架構範例

### 12.1 典型叢集配置

```
┌─────────────────────────────────────┐
│        MacBook Pro M4 Max           │
│  (Master + API + Worker + Runner)   │
└────────────┬────────────────────────┘
             │ Thunderbolt 5 RDMA
             │
┌────────────▼────────────────────────┐
│        Mac Studio M3 Ultra          │
│  (Worker + Runner) × 3 Runners      │
└────────────┬────────────────────────┘
             │ Thunderbolt 5 RDMA
             │
┌────────────▼────────────────────────┐
│        Mac Studio M3 Ultra          │
│  (Worker + Runner) × 3 Runners      │
└─────────────────────────────────────┘
```

### 12.2 DeepSeek V3.1 部署範例

```
模型：DeepSeek V3.1 671B (8-bit)
總大小：713 GB
層數：61

Pipeline + Tensor 並行：
├── Device 0: Layers 0-20 + Tensor split
├── Device 1: Layers 20-40 + Tensor split
└── Device 2: Layers 40-61 + Tensor split

預期效能：
- 吞吐量：~3.2× 加速 vs 單設備
- 延遲：<1μs (RDMA) vs ~100μs (TCP)
```

---

## 13. 關鍵技術亮點

### 13.1 架構設計

| 特點 | 說明 |
|------|------|
| Event Sourcing + CQRS | 完全可審計的系統狀態變化歷史 |
| Pydantic 型別安全 | 所有資料模型強型別（pyright strict） |
| Anyio 非同步架構 | 與執行時無關的非同步程式碼 |
| 零配置叢集 | mDNS 自動發現 + 持久化節點身份 |

### 13.2 效能優化

| 優化 | 說明 |
|------|------|
| Pipeline 並行 | 層級分配，降低單設備記憶體需求 |
| Tensor 並行 | 權重分片，加速計算 |
| KV Cache 量化 | 減少記憶體佔用 |
| RDMA over Thunderbolt | 超低延遲通訊 |

### 13.3 可靠性

| 機制 | 說明 |
|------|------|
| 進程隔離 | Runner 在獨立進程中執行 |
| 自動選舉 | Master 故障自動恢復 |
| 事件重放 | 支援狀態重建 |
| 優雅降級 | 單節點也能運行 |

---

## 14. 關鍵檔案路徑索引

| 功能 | 路徑 |
|------|------|
| 系統入口 | `src/exo/main.py` |
| Master 核心 | `src/exo/master/main.py` |
| Worker 核心 | `src/exo/worker/main.py` |
| 放置演算法 | `src/exo/master/placement.py` |
| 狀態機計畫 | `src/exo/worker/plan.py` |
| 推理引擎 | `src/exo/worker/engines/mlx/generator/generate.py` |
| API 端點 | `src/exo/master/api.py` |
| 事件套用 | `src/exo/shared/apply.py` |
| 選舉演算法 | `src/exo/shared/election.py` |
| 拓撲管理 | `src/exo/shared/topology.py` |
| Rust 網路 | `rust/exo_pyo3_bindings/src/networking.rs` |
| 模型卡片 | `src/exo/shared/models/model_cards.py` |
| 測試套件 | `src/exo/{master,worker,shared}/tests/` |
| 常數定義 | `src/exo/shared/constants.py` |
| 類型定義 | `src/exo/shared/types/` |

---

## 15. 類型系統繼承關係

```
CamelCaseModel (Pydantic BaseModel)
├── TaggedModel (JSON tagged union)
│   ├── BaseEvent
│   │   ├── TestEvent, TaskCreated, RunnerStatusUpdated...
│   │   └── Event = Union[...]
│   ├── BaseCommand
│   │   ├── PlaceInstance, ChatCompletion...
│   │   └── Command = Union[...]
│   ├── BaseTask
│   │   ├── CreateRunner, DownloadModel...
│   │   └── Task = Union[...]
│   └── BaseRunnerStatus
│       ├── RunnerIdle, RunnerRunning...
│       └── RunnerStatus = Union[...]
└── 其他資料類
    ├── Instance, BoundInstance
    ├── ShardMetadata (Pipeline/Tensor)
    ├── State, IndexedEvent
    └── ModelMetadata, Memory
```

---

*本文件最後更新：2026-01-15*
*EXO 版本：基於 commit c1be518*
