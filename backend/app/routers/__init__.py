"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import batch_load, resource

# 批量导入有固定前缀的子路径，必须比 /api/{module} 更早注册。
ROUTERS = [batch_load, resource]
