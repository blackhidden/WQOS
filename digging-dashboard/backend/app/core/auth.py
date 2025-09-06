"""
JWT认证核心功能
"""

from datetime import datetime, timedelta
from typing import Optional
import jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.config import get_settings
from app.core.exceptions import AuthenticationError
from app.db.models import DashboardUser
from app.db.database import get_db

settings = get_settings()

# 密码加密上下文
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# HTTP Bearer token 方案
security = HTTPBearer()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """验证密码"""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """生成密码哈希"""
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """创建访问令牌"""
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.access_token_expire_minutes)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)
    return encoded_jwt


def verify_token(token: str) -> dict:
    """验证令牌"""
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        username: str = payload.get("sub")
        user_id: int = payload.get("user_id")
        
        if username is None or user_id is None:
            raise AuthenticationError("无效的令牌")
        
        return {"username": username, "user_id": user_id}
    except jwt.ExpiredSignatureError:
        raise AuthenticationError("令牌已过期")
    except jwt.JWTError:
        raise AuthenticationError("无效的令牌")


def authenticate_user(db: Session, username: str, password: str) -> Optional[DashboardUser]:
    """验证用户"""
    user = db.query(DashboardUser).filter(DashboardUser.username == username).first()
    
    if not user:
        return None
    
    if not user.is_active:
        return None
    
    if not verify_password(password, user.password_hash):
        return None
    
    # 更新最后登录时间
    user.last_login = datetime.utcnow()
    db.commit()
    
    return user


def get_user_by_username(db: Session, username: str) -> Optional[DashboardUser]:
    """根据用户名获取用户"""
    return db.query(DashboardUser).filter(DashboardUser.username == username).first()


def get_user_by_id(db: Session, user_id: int) -> Optional[DashboardUser]:
    """根据用户ID获取用户"""
    return db.query(DashboardUser).filter(DashboardUser.id == user_id).first()


def create_user(db: Session, username: str, password: str, email: Optional[str] = None) -> DashboardUser:
    """创建用户"""
    # 检查用户名是否已存在
    existing_user = get_user_by_username(db, username)
    if existing_user:
        raise AuthenticationError("用户名已存在")
    
    # 创建新用户
    hashed_password = get_password_hash(password)
    user = DashboardUser(
        username=username,
        password_hash=hashed_password,
        email=email
    )
    
    db.add(user)
    db.commit()
    db.refresh(user)
    
    return user


def change_password(db: Session, user_id: int, current_password: str, new_password: str) -> bool:
    """修改密码"""
    user = get_user_by_id(db, user_id)
    if not user:
        raise AuthenticationError("用户不存在")
    
    if not verify_password(current_password, user.password_hash):
        raise AuthenticationError("当前密码错误")
    
    # 更新密码
    user.password_hash = get_password_hash(new_password)
    db.commit()
    
    return True


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> DashboardUser:
    """获取当前用户（依赖注入）"""
    try:
        # 从Bearer token中提取token
        token = credentials.credentials
        
        # 验证token
        payload = verify_token(token)
        username = payload["username"]
        user_id = payload["user_id"]
        
        # 从数据库获取用户
        user = get_user_by_id(db, user_id)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="用户不存在",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        if not user.is_active:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="用户已被禁用",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        return user
        
    except AuthenticationError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"}
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="认证失败",
            headers={"WWW-Authenticate": "Bearer"}
        )


def get_current_active_user(current_user: DashboardUser = Depends(get_current_user)) -> DashboardUser:
    """获取当前活跃用户（依赖注入）"""
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="用户已被禁用"
        )
    return current_user
