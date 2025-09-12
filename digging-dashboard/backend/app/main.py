"""
FastAPI 应用主入口
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import time
import structlog

from app.config import get_settings
from app.api.auth import router as auth_router
from app.api.config import router as config_router
from app.api.process import router as process_router
from app.api.logs import router as logs_router
from app.api.websocket import router as websocket_router
from app.api.scripts import router as scripts_router
from app.api.alphas import router as alphas_router
from app.api.dataset import router as dataset_router
from app.core.exceptions import DashboardException
from app.db.database import create_tables
from app.db import worldquant_config  # 导入WorldQuant配置模型

# 配置结构化日志
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# 获取设置
settings = get_settings()

# 创建FastAPI应用
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="WorldQuant Alpha 挖掘脚本控制面板 API",
    debug=settings.debug,
)


# CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 请求日志中间件
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """记录请求日志"""
    start_time = time.time()
    
    # 记录请求开始
    logger.info(
        "request_start",
        method=request.method,
        url=str(request.url),
        client_ip=request.client.host if request.client else None
    )
    
    # 处理请求
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        
        # 记录请求完成
        logger.info(
            "request_complete",
            method=request.method,
            url=str(request.url),
            status_code=response.status_code,
            process_time=round(process_time, 4)
        )
        
        # 添加响应头
        response.headers["X-Process-Time"] = str(process_time)
        return response
        
    except Exception as e:
        process_time = time.time() - start_time
        
        # 记录请求错误
        logger.error(
            "request_error",
            method=request.method,
            url=str(request.url),
            error=str(e),
            process_time=round(process_time, 4)
        )
        raise


# 全局异常处理器
@app.exception_handler(DashboardException)
async def dashboard_exception_handler(request: Request, exc: DashboardException):
    """处理自定义异常"""
    logger.error(
        "dashboard_exception",
        error_code=exc.error_code,
        message=exc.message,
        details=exc.details,
        url=str(request.url)
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.error_code,
            "message": exc.message,
            "details": exc.details
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """处理一般异常"""
    logger.error(
        "unhandled_exception",
        error=str(exc),
        error_type=type(exc).__name__,
        url=str(request.url)
    )
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "INTERNAL_SERVER_ERROR",
            "message": "服务器内部错误",
            "details": str(exc) if settings.debug else None
        }
    )


# 应用启动事件
@app.on_event("startup")
async def startup_event():
    """应用启动时执行"""
    logger.info(
        "application_startup",
        app_name=settings.app_name,
        version=settings.app_version,
        debug=settings.debug,
        port=settings.port
    )
    
    # 创建数据库表
    try:
        create_tables()
        logger.info("database_tables_created")
    except Exception as e:
        logger.error("database_setup_failed", error=str(e))
        raise


# 应用关闭事件
@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时执行"""
    logger.info("application_shutdown")


# 健康检查端点
@app.get("/health")
async def health_check():
    """健康检查"""
    return {
        "status": "healthy",
        "app_name": settings.app_name,
        "version": settings.app_version,
        "timestamp": time.time()
    }


# 根路径
@app.get("/")
async def root():
    """根路径欢迎信息"""
    return {
        "message": "WorldQuant Alpha 挖掘控制面板 API",
        "version": settings.app_version,
        "docs": "/docs",
        "health": "/health"
    }


# 注册API路由
app.include_router(auth_router, prefix="/api/auth", tags=["认证"])
app.include_router(config_router, prefix="/api/config", tags=["配置管理"])
app.include_router(process_router, prefix="/api/process", tags=["进程控制"])
app.include_router(scripts_router, prefix="/api/scripts", tags=["脚本管理"])
app.include_router(logs_router, prefix="/api/logs", tags=["日志管理"])
app.include_router(alphas_router, prefix="/api/alphas", tags=["Alpha管理"])
app.include_router(dataset_router, prefix="/api", tags=["数据集管理"])
app.include_router(websocket_router, prefix="/ws", tags=["WebSocket"])


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    )
