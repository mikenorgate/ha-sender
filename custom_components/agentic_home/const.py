"""Constants for the Agentic Home integration."""

DOMAIN = "agentic_home"

# Config entry keys
CONF_INGRESS_URL = "ingress_url"
CONF_JWT_TOKEN = "jwt_token"
CONF_EXCLUDE_ENTITIES = "exclude_entities"

# Ingress API paths (must match services/ingress/internal/http/handler.go RouterConfigFunc)
INGRESS_STATUS_PATH = "/api/v1/ingress/status"
INGRESS_STREAM_PATH = "/api/v1/ingress/stream"
INGRESS_REGISTRY_PATH = "/api/v1/ingress/registry"

# Batch flush parameters
BATCH_FLUSH_INTERVAL_MS = 300
BATCH_MAX_FRAMES = 100

# HTTP client timeout (seconds)
HTTP_TIMEOUT_SECONDS = 10

# Inventory/device snapshot cadence
INVENTORY_INTERVAL_SECONDS = 3600
INVENTORY_JITTER_SECONDS = 120

# Heartbeat cadence (seconds)
HEARTBEAT_INTERVAL_SECONDS = 10

# Connection binary sensor staleness check interval (seconds)
STALENESS_CHECK_SECONDS = 15