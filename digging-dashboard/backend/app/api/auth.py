"""
认证API路由
"""

from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from app.config import get_settings
from app.core.auth import authenticate_user, create_access_token, verify_token, get_user_by_id
from app.core.exceptions import AuthenticationError
from app.db.database import get_db
from app.models.auth import LoginRequest, TokenResponse, UserInfo

router = APIRouter()
security = HTTPBearer()
settings = get_settings()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> UserInfo:
    """获取当前用户（依赖注入）"""
    try:
        token_data = verify_token(credentials.credentials)
        user = get_user_by_id(db, token_data["user_id"])
        
        if user is None:
            raise AuthenticationError("用户不存在")
        
        return UserInfo.from_orm(user)
    
    except AuthenticationError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        )


@router.post("/login", response_model=TokenResponse)
async def login(login_request: LoginRequest, db: Session = Depends(get_db)):
    """用户登录"""
    user = authenticate_user(db, login_request.username, login_request.password)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户名或密码错误",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # 创建访问令牌
    access_token_expires = timedelta(minutes=settings.access_token_expire_minutes)
    access_token = create_access_token(
        data={"sub": user.username, "user_id": user.id},
        expires_delta=access_token_expires
    )
    
    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        expires_in=settings.access_token_expire_minutes * 60,
        user=UserInfo.from_orm(user)
    )


@router.post("/logout")
async def logout(current_user: UserInfo = Depends(get_current_user)):
    """用户登出"""
    # 注意：JWT是无状态的，这里只是提供一个端点
    # 实际的登出需要在前端删除token
    return {"message": "登出成功"}


@router.get("/me", response_model=UserInfo)
async def get_current_user_info(current_user: UserInfo = Depends(get_current_user)):
    """获取当前用户信息"""
    return current_user


@router.post("/verify-token")
async def verify_token_endpoint(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    """验证令牌有效性"""
    try:
        token_data = verify_token(credentials.credentials)
        user = get_user_by_id(db, token_data["user_id"])
        
        if user is None:
            raise AuthenticationError("用户不存在")
        
        return {
            "valid": True,
            "user_id": user.id,
            "username": user.username
        }
    
    except AuthenticationError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="无效的令牌",
            headers={"WWW-Authenticate": "Bearer"},
        )
