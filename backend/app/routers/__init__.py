"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import cross_module, completion, resource

# 固定前缀路由（/api/cross、/api/completion）必须排在通配 /api/{module} 之前，
# 否则会被资源路由的 /{module}/{entry_id} 抢先匹配。
ROUTERS = [cross_module, completion, resource]
